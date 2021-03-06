from datacube.model import Dataset
from datacube.model import MetadataType


def test_crs_parse(indexed_ls5_scene_dataset_types):
    # type: (MetadataType) -> None
    product = indexed_ls5_scene_dataset_types[2]

    # Explicit CRS, should load fine.
    # Taken from LS8_OLI_NBAR_3577_-14_-11_20140601021126000000.nc
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "valid_data": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1396453.986271351, -1100000.0], [-1400000.0, -1100000.0],
                         [-1400000.0, -1053643.4714392645], [-1392296.4215373022, -1054399.795365491],
                         [-1390986.9858215596, -1054531.808155645],
                         [-1390806.366757733, -1054585.3982497198],
                         [-1396453.986271351, -1100000.0]]
                    ]
                },
                "geo_ref_points": {
                    "ll": {"x": -1400000.0, "y": -1100000.0},
                    "lr": {"x": -1300000.0, "y": -1100000.0},
                    "ul": {"x": -1400000.0, "y": -1000000.0},
                    "ur": {"x": -1300000.0, "y": -1000000.0}},
                "spatial_reference": "EPSG:3577"
            }
        }

    }, local_uri=None)
    assert str(d.crs) == 'EPSG:3577'
    assert d.extent is not None

    # Valid datum/zone as seen on our LS5 scene, should infer crs.
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "zone": -51,
                "datum": "GDA94",
                "ellipsoid": "GRS80",
                "orientation": "NORTH_UP",
                "geo_ref_points": {
                    "ll": {"x": 537437.5, "y": 5900512.5},
                    "lr": {"x": 781687.5, "y": 5900512.5},
                    "ul": {"x": 537437.5, "y": 6117112.5},
                    "ur": {"x": 781687.5, "y": 6117112.5}
                },
                "map_projection": "UTM",
                "resampling_option": "CUBIC_CONVOLUTION"
            }
        }
    }, local_uri=None)
    assert str(d.crs) == 'EPSG:28351'
    assert d.extent is not None

    # No projection specified in the dataset
    d = Dataset(product, {}, local_uri=None)
    assert d.crs is None
    assert d.extent is None

    # Invalid datum/zone, can't infer
    d = Dataset(product, {
        "grid_spatial": {
            "projection": {
                "zone": -60,
                "datum": "GDA94",
                "ellipsoid": "GRS80",
                "orientation": "NORTH_UP",
                "geo_ref_points": {
                    "ll": {"x": 537437.5, "y": 5900512.5},
                    "lr": {"x": 781687.5, "y": 5900512.5},
                    "ul": {"x": 537437.5, "y": 6117112.5},
                    "ur": {"x": 781687.5, "y": 6117112.5}
                },
                "map_projection": "UTM",
                "resampling_option": "CUBIC_CONVOLUTION"
            }
        }
    }, local_uri=None)
    # Prints warning: Can't figure out projection: possibly invalid zone (-60) for datum ('GDA94')."
    # We still return None, rather than error, as they didn't specify a CRS explicitly
    assert d.crs is None
