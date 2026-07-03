// Package geocoder reverse-geocodes earthquake epicenters offline, porting
// src/mmeq/analysis/geocoder.py with golden parity.
//
// It produces the 6 published columns (state_region, district, township,
// nearest_city, place_type, distance_km) from:
//
//   - geoBoundaries admin polygons (data/admin/mmr_admin{1,2,3}.geojson,
//     ADM1=state, ADM2=district, ADM3=township), matched by point-in-polygon
//     over features in file order (first match wins, "" when none) — mirroring
//     shapely's STRtree candidates + geom.contains(Point).
//   - ~74k OSM place nodes (data/osm/myanmar_places.csv), nearest by 3D
//     unit-sphere Euclidean distance (equivalent to great-circle nearest, and
//     to scipy cKDTree over the same embedding), distance reported via
//     haversine (R=6371 km) rounded half-to-even to 1 decimal, matching
//     Python's round(d, 1).
//
// Boundary semantics: shapely's contains() treats a point exactly on a
// polygon edge or vertex as NOT contained (strict interior). The hand-rolled
// even-odd ray casting used here is ambiguous exactly on edges/vertices, so
// results may differ from Python for points precisely on an admin boundary.
// Such points are astronomically unlikely with real epicenter coordinates
// against this data; interior/exterior points match exactly (golden-tested).
package geocoder

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/akzedevops/mmeqopendata/go/internal/catalog"
)

// earthRadiusKm mirrors EARTH_R in geocoder.py.
const earthRadiusKm = 6371.0

// point is a lon/lat position (GeoJSON axis order).
type point struct{ x, y float64 }

// ring is a closed linear ring.
type ring []point

// polygon is one GeoJSON Polygon: rings[0] is the exterior, the rest are
// holes. The bbox covers the exterior ring for a cheap pre-check.
type polygon struct {
	rings                  []ring
	minX, minY, maxX, maxY float64
}

// contains reports whether (x=lon, y=lat) is inside the exterior ring and
// outside every hole, using even-odd ray casting.
func (p *polygon) contains(x, y float64) bool {
	if x < p.minX || x > p.maxX || y < p.minY || y > p.maxY {
		return false
	}
	if !inRing(p.rings[0], x, y) {
		return false
	}
	for _, hole := range p.rings[1:] {
		if inRing(hole, x, y) {
			return false
		}
	}
	return true
}

// inRing is the classic even-odd (crossing-number) test: cast a ray in +x and
// count edge crossings. Points exactly on an edge give unspecified results
// (see package doc on boundary semantics).
func inRing(r ring, x, y float64) bool {
	inside := false
	for i, j := 0, len(r)-1; i < len(r); j, i = i, i+1 {
		xi, yi := r[i].x, r[i].y
		xj, yj := r[j].x, r[j].y
		if (yi > y) != (yj > y) && x < (xj-xi)*(y-yi)/(yj-yi)+xi {
			inside = !inside
		}
	}
	return inside
}

// adminFeature is one named admin area (Polygon or MultiPolygon).
type adminFeature struct {
	name  string
	polys []polygon
}

// adminLayer is one ADM level, features kept in file order.
type adminLayer struct {
	feats []adminFeature
}

// query returns the shapeName of the first feature (file order) containing
// the point, or "" — mirroring _query_admin in geocoder.py.
func (l *adminLayer) query(lat, lon float64) string {
	for i := range l.feats {
		f := &l.feats[i]
		for j := range f.polys {
			if f.polys[j].contains(lon, lat) {
				return f.name
			}
		}
	}
	return ""
}

// place is one OSM place node with its precomputed 3D unit-sphere embedding.
type place struct {
	name, placeType string
	lat, lon        float64
	x, y, z         float64
}

// Geocoder holds eagerly loaded admin layers and OSM places. Missing data
// files leave the corresponding layer empty (queries return zero values),
// matching the Python module's warn-and-degrade behavior.
type Geocoder struct {
	adm1, adm2, adm3 adminLayer
	places           []place
}

// New loads all admin levels and the places CSV from dataDir (the repo's
// data/ directory). Missing files log a warning and are skipped, not errors;
// malformed files are errors.
func New(dataDir string) (*Geocoder, error) {
	g := &Geocoder{}
	layers := []struct {
		dst  *adminLayer
		path string
	}{
		{&g.adm1, filepath.Join(dataDir, "admin", "mmr_admin1.geojson")},
		{&g.adm2, filepath.Join(dataDir, "admin", "mmr_admin2.geojson")},
		{&g.adm3, filepath.Join(dataDir, "admin", "mmr_admin3.geojson")},
	}
	for _, l := range layers {
		layer, err := loadAdmin(l.path)
		if err != nil {
			return nil, err
		}
		*l.dst = layer
	}
	places, err := loadPlaces(filepath.Join(dataDir, "osm", "myanmar_places.csv"))
	if err != nil {
		return nil, err
	}
	g.places = places
	return g, nil
}

// geoFeatureCollection mirrors the subset of GeoJSON we consume.
type geoFeatureCollection struct {
	Features []struct {
		Properties struct {
			ShapeName string `json:"shapeName"`
		} `json:"properties"`
		Geometry struct {
			Type        string          `json:"type"`
			Coordinates json.RawMessage `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
}

func loadAdmin(path string) (adminLayer, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("geocoder: admin boundaries not found: %s", path)
		return adminLayer{}, nil
	}
	if err != nil {
		return adminLayer{}, err
	}
	var fc geoFeatureCollection
	if err := json.Unmarshal(raw, &fc); err != nil {
		return adminLayer{}, fmt.Errorf("geocoder: parse %s: %w", path, err)
	}
	layer := adminLayer{feats: make([]adminFeature, 0, len(fc.Features))}
	for i, f := range fc.Features {
		feat := adminFeature{name: f.Properties.ShapeName}
		switch f.Geometry.Type {
		case "Polygon":
			var coords [][][]float64
			if err := json.Unmarshal(f.Geometry.Coordinates, &coords); err != nil {
				return adminLayer{}, fmt.Errorf("geocoder: %s feature %d: %w", path, i, err)
			}
			p, err := buildPolygon(coords)
			if err != nil {
				return adminLayer{}, fmt.Errorf("geocoder: %s feature %d: %w", path, i, err)
			}
			feat.polys = []polygon{p}
		case "MultiPolygon":
			var coords [][][][]float64
			if err := json.Unmarshal(f.Geometry.Coordinates, &coords); err != nil {
				return adminLayer{}, fmt.Errorf("geocoder: %s feature %d: %w", path, i, err)
			}
			for _, pc := range coords {
				p, err := buildPolygon(pc)
				if err != nil {
					return adminLayer{}, fmt.Errorf("geocoder: %s feature %d: %w", path, i, err)
				}
				feat.polys = append(feat.polys, p)
			}
		default:
			return adminLayer{}, fmt.Errorf("geocoder: %s feature %d: unsupported geometry %q",
				path, i, f.Geometry.Type)
		}
		layer.feats = append(layer.feats, feat)
	}
	return layer, nil
}

func buildPolygon(coords [][][]float64) (polygon, error) {
	if len(coords) == 0 {
		return polygon{}, errors.New("polygon with no rings")
	}
	p := polygon{rings: make([]ring, 0, len(coords))}
	for ri, rc := range coords {
		r := make(ring, 0, len(rc))
		for _, pos := range rc {
			if len(pos) < 2 {
				return polygon{}, errors.New("position with fewer than 2 coordinates")
			}
			r = append(r, point{x: pos[0], y: pos[1]})
		}
		if len(r) < 4 {
			return polygon{}, errors.New("ring with fewer than 4 positions")
		}
		if ri == 0 {
			p.minX, p.minY = r[0].x, r[0].y
			p.maxX, p.maxY = r[0].x, r[0].y
			for _, pt := range r[1:] {
				p.minX = math.Min(p.minX, pt.x)
				p.maxX = math.Max(p.maxX, pt.x)
				p.minY = math.Min(p.minY, pt.y)
				p.maxY = math.Max(p.maxY, pt.y)
			}
		}
		p.rings = append(p.rings, r)
	}
	return p, nil
}

func loadPlaces(path string) ([]place, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("geocoder: places file not found: %s", path)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("geocoder: read %s header: %w", path, err)
	}
	col := make(map[string]int, len(header))
	for i, h := range header {
		col[h] = i
	}
	for _, need := range []string{"name", "name_en", "place", "lat", "lon"} {
		if _, ok := col[need]; !ok {
			return nil, fmt.Errorf("geocoder: %s missing column %q", path, need)
		}
	}

	var places []place
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("geocoder: read %s: %w", path, err)
		}
		lat, err := strconv.ParseFloat(rec[col["lat"]], 64)
		if err != nil {
			return nil, fmt.Errorf("geocoder: %s bad lat %q: %w", path, rec[col["lat"]], err)
		}
		lon, err := strconv.ParseFloat(rec[col["lon"]], 64)
		if err != nil {
			return nil, fmt.Errorf("geocoder: %s bad lon %q: %w", path, rec[col["lon"]], err)
		}
		name := rec[col["name_en"]]
		if name == "" { // Python: row["name_en"] or row["name"]
			name = rec[col["name"]]
		}
		rlat, rlon := radians(lat), radians(lon)
		places = append(places, place{
			name:      name,
			placeType: rec[col["place"]],
			lat:       lat,
			lon:       lon,
			x:         math.Cos(rlat) * math.Cos(rlon),
			y:         math.Cos(rlat) * math.Sin(rlon),
			z:         math.Sin(rlat),
		})
	}
	log.Printf("geocoder: loaded %d OSM places", len(places))
	return places, nil
}

// State returns the ADM1 state/region name containing the point, or "".
func (g *Geocoder) State(lat, lon float64) string { return g.adm1.query(lat, lon) }

// District returns the ADM2 district name containing the point, or "".
func (g *Geocoder) District(lat, lon float64) string { return g.adm2.query(lat, lon) }

// Township returns the ADM3 township name containing the point, or "".
func (g *Geocoder) Township(lat, lon float64) string { return g.adm3.query(lat, lon) }

// NearestPlace returns the nearest OSM place (name, place type, haversine
// distance in km rounded half-to-even to 1 decimal). With no places loaded it
// returns ("", "", 0.0), like the Python fallback. Nearest is by 3D
// unit-sphere Euclidean distance; ties keep the lowest index, matching scipy
// cKDTree.
func (g *Geocoder) NearestPlace(lat, lon float64) (name, placeType string, distKm float64) {
	if len(g.places) == 0 {
		return "", "", 0.0
	}
	rlat, rlon := radians(lat), radians(lon)
	qx := math.Cos(rlat) * math.Cos(rlon)
	qy := math.Cos(rlat) * math.Sin(rlon)
	qz := math.Sin(rlat)
	best, bestD := 0, math.Inf(1)
	for i := range g.places {
		p := &g.places[i]
		dx, dy, dz := p.x-qx, p.y-qy, p.z-qz
		if d := dx*dx + dy*dy + dz*dz; d < bestD { // strict <: first minimum wins
			best, bestD = i, d
		}
	}
	p := &g.places[best]
	return p.name, p.placeType, round1(haversineKm(lat, lon, p.lat, p.lon))
}

// Enrich sets the 6 published geocoding fields on q.Extra, mirroring
// enrich_dataframe: missing admin data yields empty strings, missing places
// data yields ""/""/0.0. distance_km is stored as float64.
func (g *Geocoder) Enrich(q *catalog.Quake) {
	if q.Extra == nil {
		q.Extra = make(map[string]any, 6)
	}
	q.Extra["state_region"] = g.State(q.Latitude, q.Longitude)
	q.Extra["district"] = g.District(q.Latitude, q.Longitude)
	q.Extra["township"] = g.Township(q.Latitude, q.Longitude)
	name, placeType, distKm := g.NearestPlace(q.Latitude, q.Longitude)
	q.Extra["nearest_city"] = name
	q.Extra["place_type"] = placeType
	q.Extra["distance_km"] = distKm
}

func radians(deg float64) float64 { return deg * (math.Pi / 180) }

// haversineKm mirrors _haversine_km: R=6371.0 and asin(min(1, sqrt(a))).
func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	rlat1, rlon1 := radians(lat1), radians(lon1)
	rlat2, rlon2 := radians(lat2), radians(lon2)
	dlat, dlon := rlat2-rlat1, rlon2-rlon1
	sLat, sLon := math.Sin(dlat/2), math.Sin(dlon/2)
	a := sLat*sLat + math.Cos(rlat1)*math.Cos(rlat2)*sLon*sLon
	return earthRadiusKm * 2 * math.Asin(math.Min(1, math.Sqrt(a)))
}

// round1 approximates Python's round(d, 1) via round-half-to-even on d*10 (NOT
// math.Round's half-away-from-zero). It is not exact decimal rounding: the d*10
// float multiplication can create exact .5 ties that Python's decimal-aware
// rounding does not see (e.g. inputs like 0.15 or 2.85 differ). It does match
// Python on the golden set and on real haversine outputs, which is what the
// parity tests pin down.
func round1(d float64) float64 { return math.RoundToEven(d*10) / 10 }
