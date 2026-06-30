// Package geocoder reverse-geocodes each event, mirroring analysis/geocoder.py.
//
// It produces the 6 published columns (state_region, district, township, nearest_city,
// place_type, distance_km) via ADM1/2/3 point-in-polygon (paulmach/orb) + nearest OSM
// place (kd-tree) + haversine. Fidelity-sensitive: must match scipy cKDTree 3D-sphere
// nearest semantics and shapely STRtree.contains boundary handling, or fall back to a
// thin Python post-step (hybrid). Implementation pending spec 002.
package geocoder
