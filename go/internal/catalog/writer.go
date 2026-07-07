package catalog

// CSV + JSON writer with golden-file parity against the Python pipeline's writer
// (src/mmeq/export/writer.py: save_to_csv / save_to_json / _dedup_frame /
// merge_combined_json). Every formatting decision here mirrors what pandas.to_csv
// and json.dump(indent=2, ensure_ascii=False) produce, byte for byte:
//
//   - Floats use the Python repr rule: shortest round-trip decimal, fixed notation
//     with a trailing ".0" for integral values (90.0 -> "90.0", 3.6 -> "3.6"),
//     switching to exponent form only when |x| >= 1e16 or 0 < |x| < 1e-4
//     (matching CPython's float_repr / numpy str), e.g. "1e+16", "9.999e-05".
//   - Missing values render as "" in CSV (na_rep default) and as the literal
//     NaN token in JSON for numeric columns / absent keys (json.dump writes
//     non-standard NaN), null for a Python None in an object (string) column.
//   - CSV quoting is csv.QUOTE_MINIMAL: quote only fields containing the
//     delimiter, the quote char, '\r' or '\n' (Go's encoding/csv additionally
//     quotes leading-whitespace fields, so a custom emitter is used), LF line
//     endings, header row included.
//   - JSON is {"earthquakes": [...]} with indent=2 and UTF-8 literals
//     (ensure_ascii=False), no trailing newline.
//
// Semantics: monthly files are overwritten with just the new frame; yearly and
// combined CSVs merge with the existing file and dedup keyed on "id" keeping the
// last occurrence (rows without an id all share pandas' NaN key; when no row has
// an id there is no id column and dedup falls back to full-row keep-first, like
// _dedup_frame); the combined JSON merges keyed on id (falling back to
// (time_utc, latitude, longitude)) preserving first-seen order with later
// records winning. Writes are atomic (temp file + rename), like _atomic_write.
//
// Column order: pandas derives it from the API response dict order, which a Go
// map cannot preserve; callers pass an explicit column order (DefaultColumns is
// the production 32-column header) and any keys not listed are appended in
// sorted order. JSON key order follows the same rule via ColumnsFor.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// DefaultColumns is the production column order of the exported catalog, taken
// from the header of the real monthly CSVs (quake_exports/csv/monthly/*.csv).
var DefaultColumns = []string{
	"latitude", "longitude", "depth", "mag", "magType", "nst", "gap", "dmin",
	"rms", "net", "id", "updated", "location", "place", "country", "continent",
	"type", "timeAdded", "timestamp", "locationInferred", "state",
	"initialPosition", "shakemapURL", "shakemapLastUpdated", "time_utc",
	"time_mmt", "state_region", "district", "township", "nearest_city",
	"place_type", "distance_km",
}

// typedColumns are the Quake fields promoted out of Extra that exist on every
// row; a cell for one of these always comes from the typed field (mirroring
// validate_quake_data, which guarantees these columns on every row). "id" is
// NOT here: pandas materializes an id column only when some record carries
// the key, and a record without it gets a NaN cell.
var typedColumns = map[string]bool{
	"time_utc": true, "time_mmt": true,
	"latitude": true, "longitude": true, "depth": true, "mag": true,
}

// cellValue returns the value for one column of one quake: the typed field for
// the promoted columns, otherwise Extra[col]. present=false means the key is
// absent entirely (pandas: NaN-filled cell for a record missing the key). The
// id cell prefers the raw Extra["id"] value (which distinguishes an explicit
// null from a missing key) and falls back to the typed ID for hand-built
// quakes; a record with no id at all reports present=false so the CSV cell
// renders "" and the JSON cell NaN, exactly like pandas.
func cellValue(q Quake, col string) (v any, present bool) {
	switch col {
	case "id":
		if v, ok := q.Extra["id"]; ok {
			return v, true
		}
		if q.ID != "" {
			return q.ID, true
		}
		return nil, false
	case "time_utc":
		return q.TimeUTC, true
	case "time_mmt":
		return q.TimeMMT, true
	case "latitude":
		return q.Latitude, true
	case "longitude":
		return q.Longitude, true
	case "depth":
		return q.Depth, true
	case "mag":
		return q.Mag, true
	}
	v, present = q.Extra[col]
	return v, present
}

// presentColumns is the set of columns the quakes actually carry: the typed
// six plus every Extra key (pandas only materializes columns present in the
// records). "id" is included only when some record carries one — a frame in
// which no record has an id has no id column at all.
func presentColumns(quakes []Quake) map[string]bool {
	p := make(map[string]bool, len(typedColumns))
	for c := range typedColumns {
		p[c] = true
	}
	for _, q := range quakes {
		if q.ID != "" {
			p["id"] = true
		}
		for k := range q.Extra {
			p[k] = true
		}
	}
	return p
}

// normalizeColumns filters `columns` to those present in the data (preserving
// order, dropping duplicates) and appends any remaining present keys in sorted
// order, so output columns always cover exactly the data like a DataFrame does.
func normalizeColumns(quakes []Quake, columns []string) []string {
	present := presentColumns(quakes)
	out := make([]string, 0, len(present))
	seen := make(map[string]bool, len(present))
	for _, c := range columns {
		if present[c] && !seen[c] {
			out = append(out, c)
			seen[c] = true
		}
	}
	rest := make([]string, 0, len(present))
	for k := range present {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

// ColumnsFor returns the canonical column order for a set of quakes:
// DefaultColumns restricted to keys present, then any unknown keys sorted.
// This is the order used for JSON key order (Python preserves the API dict
// order, which Go maps lose; production key order equals DefaultColumns).
func ColumnsFor(quakes []Quake) []string {
	return normalizeColumns(quakes, DefaultColumns)
}

// ---------------------------------------------------------------------------
// Python float repr
// ---------------------------------------------------------------------------

// pyFloatRepr formats f exactly like CPython's repr(float) — which is what both
// pandas.to_csv (no float_format) and json.dump emit. Rule: shortest
// round-trip digits; fixed notation with ".0" appended for integral values;
// exponent notation (two-digit minimum exponent, matching Go's 'e') only when
// |f| >= 1e16 or 0 < |f| < 1e-4. Verified against pandas 2.3.3 to_csv for:
// 90.0, 3.6, 24.441, 1e16, 9999999999999998.0, 1e-4, 9.999e-5, -0.0,
// 1782667764000.0, 0.1, 1234567.0.
func pyFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	if abs := math.Abs(f); f != 0 && (abs >= 1e16 || abs < 1e-4) {
		return strconv.FormatFloat(f, 'e', -1, 64)
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// ---------------------------------------------------------------------------
// CSV rendering (pandas to_csv parity)
// ---------------------------------------------------------------------------

// renderCSVCell renders one cell like pandas to_csv with na_rep="":
// missing/None/NaN -> "", floats via the Python repr rule, strings verbatim,
// bools as "True"/"False", json.Number literals passed through.
func renderCSVCell(v any, present bool) string {
	if !present || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if math.IsNaN(x) {
			return ""
		}
		return pyFloatRepr(x)
	case float32:
		return renderCSVCell(float64(x), true)
	case json.Number:
		return x.String()
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return fmt.Sprint(x)
	}
}

// csvField applies csv.QUOTE_MINIMAL: quote only if the field contains the
// delimiter, the quote char, or a line-break character. (Unlike Go's
// encoding/csv, leading/trailing whitespace does NOT force quoting — Python
// leaves ` lead` unquoted.)
func csvField(s string) string {
	if strings.ContainsAny(s, ",\"\r\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

// writeCSVRow appends one row with LF line ending. A row consisting of a single
// empty field is written as `""` — Python's csv writer quotes it so the row is
// not mistaken for a blank line (pandas does this for one-column NaN rows).
func writeCSVRow(b *bytes.Buffer, cells []string) {
	if len(cells) == 1 && cells[0] == "" {
		b.WriteString(`""` + "\n")
		return
	}
	for i, c := range cells {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(csvField(c))
	}
	b.WriteByte('\n')
}

// atomicWrite writes data via a temp file in the same directory then renames it
// into place, mirroring writer._atomic_write.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".mmeq-write-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	// fsync before rename: a rename can persist while the data does not after a
	// crash, leaving a truncated/empty artifact that loadCombinedRecords treats
	// as empty and then overwrites the accumulated store (2026-07-06 audit).
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	// os.CreateTemp creates the file 0600, and that mode survives the rename —
	// open the artifact up to the usual 0644 so other users/processes can read it.
	if err := os.Chmod(tmpName, 0o644); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// renderCSV renders header + rows for the given quakes and column order.
func renderCSV(quakes []Quake, cols []string) []byte {
	var buf bytes.Buffer
	writeCSVRow(&buf, cols)
	cells := make([]string, len(cols))
	for _, q := range quakes {
		for i, c := range cols {
			v, present := cellValue(q, c)
			cells[i] = renderCSVCell(v, present)
		}
		writeCSVRow(&buf, cells)
	}
	return buf.Bytes()
}

// WriteMonthlyCSV overwrites path with just the given quakes (monthly files are
// re-fetched in full each run — save_to_csv(..., overwrite=True), no dedup).
// A nil/empty slice is a no-op, like the df.empty early return.
func WriteMonthlyCSV(quakes []Quake, path string, columns []string) error {
	if len(quakes) == 0 {
		return nil
	}
	return atomicWrite(path, renderCSV(quakes, normalizeColumns(quakes, columns)))
}

// readCSVRows reads an existing CSV into a header plus rows-as-string-maps.
// Mirrors pd.read_csv(on_bad_lines="skip"): rows with extra fields are skipped,
// short rows are padded (pandas NaN-fills them). Returns nil header if the file
// does not exist.
func readCSVRows(path string) (header []string, rows []map[string]string, err error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	records, err := parseCSV(data)
	if err != nil {
		return nil, nil, err
	}
	if len(records) == 0 {
		return nil, nil, nil
	}
	header = records[0]
	for _, rec := range records[1:] {
		if len(rec) > len(header) {
			continue // on_bad_lines="skip"
		}
		row := make(map[string]string, len(header))
		for i, col := range header {
			if i < len(rec) {
				row[col] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	return header, rows, nil
}

// parseCSV is a minimal RFC-4180 reader (quoted fields, doubled quotes, LF or
// CRLF) sufficient for files this writer or pandas produced.
func parseCSV(data []byte) ([][]string, error) {
	var records [][]string
	var field bytes.Buffer
	var row []string
	inQuotes := false
	pushField := func() {
		row = append(row, field.String())
		field.Reset()
	}
	pushRow := func() {
		pushField()
		records = append(records, row)
		row = nil
	}
	for i := 0; i < len(data); i++ {
		c := data[i]
		if inQuotes {
			if c == '"' {
				if i+1 < len(data) && data[i+1] == '"' {
					field.WriteByte('"')
					i++
				} else {
					inQuotes = false
				}
			} else {
				field.WriteByte(c)
			}
			continue
		}
		switch c {
		case '"':
			inQuotes = true
		case ',':
			pushField()
		case '\r':
			// swallow; the following \n ends the row
		case '\n':
			pushRow()
		default:
			field.WriteByte(c)
		}
	}
	if inQuotes {
		return nil, fmt.Errorf("catalog: unterminated quoted CSV field")
	}
	if field.Len() > 0 || len(row) > 0 {
		pushRow()
	}
	return records, nil
}

// WriteMergedCSV merges quakes with any existing CSV at path and dedups like
// writer._dedup_frame — save_to_csv(..., dedup=True) for the accumulating
// yearly/combined files. When the merged frame has an id column, rows dedup
// keyed on "id" keeping the LAST occurrence; rows without an id (empty cell in
// the existing file — read_csv parses that as NaN — or a quake missing the
// key) all collapse into one NaN group, matching pandas. When neither the
// existing file nor the quakes carry an id column, it falls back to full-row
// dedup keeping the FIRST occurrence, like drop_duplicates(). Existing rows
// pass through as strings (pandas re-parses and re-serializes them, which is
// an identity round trip for files this pipeline wrote). Column order is the
// existing header followed by any new columns not yet present, matching
// pd.concat's union. Empty input is a no-op (the existing file is left
// untouched).
func WriteMergedCSV(quakes []Quake, path string, columns []string) error {
	if len(quakes) == 0 {
		return nil
	}
	header, existingRows, err := readCSVRows(path)
	if err != nil {
		return err
	}
	newCols := normalizeColumns(quakes, columns)
	outCols := header
	if outCols == nil {
		outCols = newCols
	} else {
		have := make(map[string]bool, len(header))
		for _, c := range header {
			have[c] = true
		}
		for _, c := range newCols {
			if !have[c] {
				outCols = append(outCols, c)
			}
		}
	}

	type mergedRow struct {
		existing map[string]string // nil => render from quake
		quake    Quake
	}
	all := make([]mergedRow, 0, len(existingRows)+len(quakes))
	for _, r := range existingRows {
		all = append(all, mergedRow{existing: r})
	}
	for _, q := range quakes {
		all = append(all, mergedRow{quake: q})
	}

	hasID := false
	for _, c := range outCols {
		if c == "id" {
			hasID = true
			break
		}
	}

	rowCells := func(r mergedRow, cells []string) {
		for j, c := range outCols {
			if r.existing != nil {
				cells[j] = r.existing[c]
			} else {
				v, present := cellValue(r.quake, c)
				cells[j] = renderCSVCell(v, present)
			}
		}
	}

	// _dedup_frame on the concatenated frame: with an id column, key on the id
	// cell (empty existing cells are NaN after read_csv, so they share the NaN
	// group with quakes missing the key); without one, key on the full row.
	keys := make([]any, len(all))
	fullRow := make([]string, len(outCols))
	for i, r := range all {
		if hasID {
			if r.existing != nil {
				if id := r.existing["id"]; id != "" {
					keys[i] = idKey{'v', id}
				} else {
					keys[i] = idKey{'m', ""} // read_csv parses "" as NaN
				}
			} else {
				keys[i] = quakeIDKey(r.quake)
			}
			continue
		}
		rowCells(r, fullRow)
		keys[i] = strings.Join(fullRow, "\x00")
	}

	// drop_duplicates: subset=["id"] keeps the LAST occurrence, full-row keeps
	// the FIRST; survivors stay at the position of the kept occurrence.
	survivor := make(map[any]int, len(keys))
	for i, k := range keys {
		if _, seen := survivor[k]; hasID || !seen {
			survivor[k] = i
		}
	}

	var buf bytes.Buffer
	writeCSVRow(&buf, outCols)
	cells := make([]string, len(outCols))
	for i, r := range all {
		if survivor[keys[i]] != i {
			continue
		}
		rowCells(r, cells)
		writeCSVRow(&buf, cells)
	}
	return atomicWrite(path, buf.Bytes())
}

// ---------------------------------------------------------------------------
// JSON rendering (json.dump(indent=2, ensure_ascii=False) parity)
// ---------------------------------------------------------------------------

// omap is an insertion-ordered JSON object: Go maps lose key order, but parity
// with Python dicts requires preserving it (existing combined-JSON records are
// re-emitted with their original key order, like json.load -> json.dump).
type omap struct {
	keys []string
	vals map[string]any
}

func newOmap(capacity int) *omap {
	return &omap{keys: make([]string, 0, capacity), vals: make(map[string]any, capacity)}
}

func (o *omap) set(k string, v any) {
	if _, ok := o.vals[k]; !ok {
		o.keys = append(o.keys, k) // duplicate keys keep first position, last value (dict semantics)
	}
	o.vals[k] = v
}

// Sentinel decoded values for Python's non-standard JSON literals: json.dump
// writes NaN/Infinity/-Infinity (the production combined JSON contains tens of
// thousands of NaN tokens), which encoding/json rejects, so sanitizePyLiterals
// swaps them for these impossible strings before decoding and the encoder
// swaps them back.
const (
	pyNaNSentinel    = "\x00NaN"
	pyInfSentinel    = "\x00Infinity"
	pyNegInfSentinel = "\x00-Infinity"
)

// sanitizePyLiterals replaces bare NaN / Infinity / -Infinity tokens (outside
// strings) with sentinel JSON strings so the standard decoder accepts them.
func sanitizePyLiterals(data []byte) []byte {
	if !bytes.Contains(data, []byte("NaN")) && !bytes.Contains(data, []byte("Infinity")) {
		return data
	}
	var out bytes.Buffer
	out.Grow(len(data) + 64)
	inStr, esc := false, false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if inStr {
			out.WriteByte(c)
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch {
		case c == '"':
			inStr = true
			out.WriteByte(c)
		case c == 'N' && bytes.HasPrefix(data[i:], []byte("NaN")):
			out.WriteString(`"\u0000NaN"`)
			i += 2
		case c == '-' && bytes.HasPrefix(data[i:], []byte("-Infinity")):
			out.WriteString(`"\u0000-Infinity"`)
			i += 8
		case c == 'I' && bytes.HasPrefix(data[i:], []byte("Infinity")):
			out.WriteString(`"\u0000Infinity"`)
			i += 7
		default:
			out.WriteByte(c)
		}
	}
	return out.Bytes()
}

// decodeOrdered decodes JSON preserving object key order and number literals
// (json.Number keeps the exact text, so 90.0 re-emits as 90.0, not 90).
func decodeOrdered(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(sanitizePyLiterals(data)))
	dec.UseNumber()
	v, err := decodeOrderedValue(dec)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func decodeOrderedValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return tok, nil // string, json.Number, bool or nil
	}
	switch d {
	case '{':
		om := newOmap(8)
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, fmt.Errorf("catalog: non-string JSON key %v", keyTok)
			}
			val, err := decodeOrderedValue(dec)
			if err != nil {
				return nil, err
			}
			om.set(key, val)
		}
		if _, err := dec.Token(); err != nil { // consume '}'
			return nil, err
		}
		return om, nil
	case '[':
		arr := []any{}
		for dec.More() {
			val, err := decodeOrderedValue(dec)
			if err != nil {
				return nil, err
			}
			arr = append(arr, val)
		}
		if _, err := dec.Token(); err != nil { // consume ']'
			return nil, err
		}
		return arr, nil
	}
	return nil, fmt.Errorf("catalog: unexpected JSON delimiter %v", d)
}

// appendPyJSONString writes s as a JSON string the way json.dump with
// ensure_ascii=False does: UTF-8 passed through literally; only `"`, `\` and
// control characters below 0x20 are escaped (short escapes where they exist,
// otherwise lowercase \u00xx). No HTML or U+2028/U+2029 escaping.
func appendPyJSONString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b.WriteString(`\"`)
		case c == '\\':
			b.WriteString(`\\`)
		case c == '\b':
			b.WriteString(`\b`)
		case c == '\f':
			b.WriteString(`\f`)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case c < 0x20:
			fmt.Fprintf(b, `\u%04x`, c)
		default:
			b.WriteByte(c) // multi-byte UTF-8 passes through verbatim
		}
	}
	b.WriteByte('"')
}

// appendJSON encodes v with Python's json.dump(indent=2) layout: objects and
// arrays open on the current line, entries at indent+2, closers at the opening
// indent; empty containers stay inline ({} / []).
func appendJSON(b *bytes.Buffer, v any, indent int) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case *omap:
		if len(x.keys) == 0 {
			b.WriteString("{}")
			return
		}
		b.WriteString("{\n")
		for i, k := range x.keys {
			writeIndent(b, indent+2)
			appendPyJSONString(b, k)
			b.WriteString(": ")
			appendJSON(b, x.vals[k], indent+2)
			if i < len(x.keys)-1 {
				b.WriteByte(',')
			}
			b.WriteByte('\n')
		}
		writeIndent(b, indent)
		b.WriteByte('}')
	case []any:
		if len(x) == 0 {
			b.WriteString("[]")
			return
		}
		b.WriteString("[\n")
		for i, item := range x {
			writeIndent(b, indent+2)
			appendJSON(b, item, indent+2)
			if i < len(x)-1 {
				b.WriteByte(',')
			}
			b.WriteByte('\n')
		}
		writeIndent(b, indent)
		b.WriteByte(']')
	case string:
		switch x {
		case pyNaNSentinel:
			b.WriteString("NaN")
		case pyInfSentinel:
			b.WriteString("Infinity")
		case pyNegInfSentinel:
			b.WriteString("-Infinity")
		default:
			appendPyJSONString(b, x)
		}
	case json.Number:
		b.WriteString(x.String())
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case float64:
		switch {
		case math.IsNaN(x):
			b.WriteString("NaN")
		case math.IsInf(x, 1):
			b.WriteString("Infinity")
		case math.IsInf(x, -1):
			b.WriteString("-Infinity")
		default:
			b.WriteString(pyFloatRepr(x))
		}
	case float32:
		appendJSON(b, float64(x), indent)
	case int:
		b.WriteString(strconv.Itoa(x))
	case int64:
		b.WriteString(strconv.FormatInt(x, 10))
	default:
		appendPyJSONString(b, fmt.Sprint(x))
	}
}

func writeIndent(b *bytes.Buffer, n int) {
	for i := 0; i < n; i++ {
		b.WriteByte(' ')
	}
}

// numericColumns reports which columns behave as pandas float columns: at least
// one present non-nil value and every present non-nil value numeric. In such a
// column a Python None coerces to NaN (json.dump -> NaN); in an object column
// None stays None (-> null). A key absent from a record is NaN either way.
func numericColumns(quakes []Quake, cols []string) map[string]bool {
	numeric := make(map[string]bool, len(cols))
	for _, c := range cols {
		hasNum, hasOther := false, false
		for _, q := range quakes {
			v, present := cellValue(q, c)
			if !present || v == nil {
				continue
			}
			switch x := v.(type) {
			case float64:
				if !math.IsNaN(x) {
					hasNum = true
				}
			case float32, int, int64, json.Number:
				hasNum = true
			default:
				hasOther = true
			}
		}
		numeric[c] = hasNum && !hasOther
	}
	return numeric
}

// renderJSONRecords renders quakes as ordered records with ColumnsFor key order
// (every record carries every column, like DataFrame.to_dict(orient="records")).
func renderJSONRecords(quakes []Quake) []*omap {
	cols := ColumnsFor(quakes)
	numeric := numericColumns(quakes, cols)
	out := make([]*omap, 0, len(quakes))
	for _, q := range quakes {
		rec := newOmap(len(cols))
		for _, c := range cols {
			v, present := cellValue(q, c)
			switch {
			case !present:
				rec.set(c, math.NaN()) // missing key -> NaN cell
			case v == nil && numeric[c]:
				rec.set(c, math.NaN()) // None in a float column -> NaN
			default:
				rec.set(c, v) // None in an object column stays null
			}
		}
		out = append(out, rec)
	}
	return out
}

func encodePayload(records []any) []byte {
	payload := newOmap(1)
	payload.set("earthquakes", records)
	var buf bytes.Buffer
	appendJSON(&buf, payload, 0)
	return buf.Bytes() // json.dump writes no trailing newline
}

// WriteJSON writes {"earthquakes": [...]} for the given quakes, overwriting
// path (save_to_json — used for monthly and yearly files). Empty input is a
// no-op.
func WriteJSON(quakes []Quake, path string) error {
	if len(quakes) == 0 {
		return nil
	}
	recs := renderJSONRecords(quakes)
	records := make([]any, len(recs))
	for i, r := range recs {
		records[i] = r
	}
	return atomicWrite(path, encodePayload(records))
}

// loadCombinedRecords mirrors load_combined_json: missing or unreadable files
// yield an empty list (the Python version logs a warning and returns []).
func loadCombinedRecords(path string) []any {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	v, err := decodeOrdered(data)
	if err != nil {
		return nil
	}
	om, ok := v.(*omap)
	if !ok {
		return nil
	}
	recs, _ := om.vals["earthquakes"].([]any)
	return recs
}

// tupleKey is the merge fallback key (time_utc, latitude, longitude) for
// records without a truthy id, mirroring merge_combined_json.
type tupleKey struct {
	timeUTC  string
	lat, lon float64
}

// pyTruthy mirrors Python truthiness for the values an id can hold
// (record.get("id") or <fallback>): None, "", 0 and False are falsy; NaN is
// truthy.
func pyTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case string:
		return x != ""
	case bool:
		return x
	case float64:
		return x != 0
	case json.Number:
		f, err := x.Float64()
		return err != nil || f != 0
	case int:
		return x != 0
	case int64:
		return x != 0
	}
	return true
}

func numVal(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

// recordKey computes the merge key for one record: a truthy id (string or
// numeric), else the (time_utc, latitude, longitude) tuple. Records whose
// fallback coordinates are missing or NaN get a unique key (a NaN Python dict
// key compares by object identity, so json-loaded NaNs never merge).
func recordKey(rec *omap, unique int) any {
	if v, ok := rec.vals["id"]; ok && pyTruthy(v) {
		switch x := v.(type) {
		case string:
			// A NaN id decoded from an existing file arrives as the NaN
			// sentinel string. Python json.load yields a distinct float nan
			// object per record (dict keys compare by identity for NaN), so
			// such records NEVER merge — give each a unique key.
			if x == pyNaNSentinel {
				return struct{ i int }{unique}
			}
			return x
		case json.Number:
			if f, err := x.Float64(); err == nil {
				if math.IsNaN(f) {
					return struct{ i int }{unique}
				}
				return f // Python int/float dict keys compare numerically
			}
			return x.String()
		case float64, int, int64, bool:
			f, _ := numVal(x)
			if b, isB := x.(bool); isB {
				if b {
					f = 1
				} else {
					f = 0
				}
			}
			// A NaN map key can be inserted but never looked up in Go, which
			// turned the final key->record resolution into a nil (a literal
			// null in the artifact, silently dropping the event). Python
			// never merges NaN ids; match that with a unique key.
			if math.IsNaN(f) {
				return struct{ i int }{unique}
			}
			return f
		}
	}
	t, _ := rec.vals["time_utc"].(string)
	lat, okLat := numVal(rec.vals["latitude"])
	lon, okLon := numVal(rec.vals["longitude"])
	if !okLat || !okLon || math.IsNaN(lat) || math.IsNaN(lon) {
		return struct{ i int }{unique}
	}
	return tupleKey{timeUTC: t, lat: lat, lon: lon}
}

// MergeCombinedJSON merges quakes into the combined JSON at existingPath and
// writes the result to outPath, exactly like the cli.py combined-JSON step:
// load existing records, merge keyed on id (fallback (time_utc, latitude,
// longitude)) preserving first-seen order with later records winning, then
// json.dump(indent=2, ensure_ascii=False). Existing records keep their
// original key order and number formatting; NaN literals survive the round
// trip. A missing existing file merges against an empty list.
func MergeCombinedJSON(existingPath string, quakes []Quake, outPath string) error {
	all := loadCombinedRecords(existingPath)
	for _, r := range renderJSONRecords(quakes) {
		all = append(all, r)
	}

	mergedByKey := make(map[any]any, len(all))
	order := make([]any, 0, len(all))
	for i, rec := range all {
		var key any
		if om, ok := rec.(*omap); ok {
			key = recordKey(om, i)
		} else {
			key = struct{ i int }{i} // non-object record: keep as-is, never merged
		}
		if _, seen := mergedByKey[key]; !seen {
			order = append(order, key)
		}
		mergedByKey[key] = rec
	}
	merged := make([]any, len(order))
	for i, k := range order {
		merged[i] = mergedByKey[k]
	}
	return atomicWrite(outPath, encodePayload(merged))
}
