// Package catalog validates, dedups, and writes the earthquake catalog.
//
// Mirrors src/mmeq/export/writer.py + parts of cli.py:cmd_export. Must reproduce the
// Python contract byte-for-byte (golden-file tested): validation bounds + NaN-drop;
// time_utc (UTC) and time_mmt (Asia/Yangon, via embedded tzdata); id-keyed dedup
// keep-last; combined-JSON merge keyed on id else (time_utc,lat,lon) preserving
// first-seen order; monthly=overwrite, yearly/combined=merge+dedup; the exact CSV
// column order and JSON shape ({"earthquakes":[...]}, indent=2, UTF-8 literal);
// atomic writes (tmp + os.Rename). Implementation pending spec 002.
package catalog
