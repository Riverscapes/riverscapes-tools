import struct
import os
from shapely.geometry import Point, Polygon, LineString


class TIN(object):

    def __init__(self, tin_path):
        self.path = tin_path

        # Required files
        self.tnxy = os.path.join(self.path, 'tnxy.adf')
        self.tnz = os.path.join(self.path, 'tnz.adf')
        self.tnod = os.path.join(self.path, 'tnod.adf')
        self.tdenv = os.path.join(self.path, 'tdenv9.adf') # tdenv for version 9
        self.tmsk = os.path.join(self.path, 'tmsk.adf')
        self.tmsx = os.path.join(self.path, 'tmsx.adf')
        self.tedg = os.path.join(self.path, 'tedg.adf')
        self.thul = os.path.join(self.path, 'thul.adf')

        # optional files
        self.prj = os.path.join(self.path, 'prj.adf') if os.path.isfile(os.path.join(self.path, 'prj.adf')) else None
        self.tndsc = os.path.join(self.path, 'tndsc.adf') if os.path.isfile(os.path.join(self.path, 'tndsc.adf')) else None
        self.tnval = os.path.join(self.path, 'tnval.adf') if os.path.isfile(os.path.join(self.path, 'tnval.adf')) else None
        self.ttdsc = os.path.join(self.path, 'ttdsc.adf') if os.path.isfile(os.path.join(self.path, 'ttdsc.adf')) else None
        self.ttval = os.path.join(self.path, 'ttval.adf') if os.path.isfile(os.path.join(self.path, 'ttval.adf')) else None

        # Arc10 Version
        self.tnodinfo = os.path.join(self.path, 'tnodinfo.adf') if os.path.isfile(os.path.join(self.path, 'tnodinfo.adf')) else None
        self.teval = os.path.join(self.path, 'teval.adf') if os.path.isfile(os.path.join(self.path, 'teval.adf')) else None

        # Load TIN
        self.header_stats = self.load_header_stats()
        self.nodes, self.supernodes = self.load_nodes(self.header_stats["number_super_points"])
        self.raw_nodes = dict(self.nodes.items() + self.supernodes.items())
        self.node_info = self.load_node_info() if self.tnodinfo else []
        self.node_tags = self.load_node_tags() if self.tnval else []
        self.triangle_indices, self.raw_indices = self.load_triangle_indices()
        self.triangles = self.generate_triangles(self.raw_nodes, self.triangle_indices)
        self.edges, self.edge_values = self.load_edges()
        self.hull_index = self.load_hull()
        self.hull_polygons = self.generate_hull_polygons(self.raw_nodes, self.hull_index)
        self.tin_nodes = self.generate_tin_nodes()
        self.breaklines = self.generate_breaklines()

        self.mask_header, self.mask = self.load_mask()
        self.mask_index_header, self.mask_index = self.load_mask_index()
        self.raw_proj = self.__load_projection() if self.prj else None

    def load_nodes(self, numbersuperpoints=None):
        coords = {}
        supercoords = {}
        with open(self.tnxy, 'rb') as f:
            with open(self.tnz, 'rb') as fz:
                i = 0
                while True:
                    chunkx = f.read(8)
                    chunky = f.read(8)
                    chunkz = fz.read(4)
                    if any(c == '' for c in [chunkx, chunky, chunkz]):
                        break
                    else:
                        i = i + 1
                        if i <= numbersuperpoints:
                            supercoords[i] = Point(struct.unpack('>d', chunkx)[0],
                                              struct.unpack('>d', chunky)[0],
                                              struct.unpack('>f', chunkz)[0])
                        else:
                            coords[i] = Point(struct.unpack('>d', chunkx)[0],
                                              struct.unpack('>d', chunky)[0],
                                              struct.unpack('>f', chunkz)[0])
        return coords, supercoords

    def load_triangle_indices(self):
        indices = {}
        raw_index = {}
        with open(self.tnod, 'rb') as f:
            i = 0
            iraw = 0
            while True:
                chunk1 = f.read(4)
                chunk2 = f.read(4)
                chunk3 = f.read(4)
                if any(c == '' for c in [chunk1, chunk2, chunk3]):
                    break
                else:
                    i = i + 1
                    indices[i] = (struct.unpack('>i', chunk1)[0],
                                  struct.unpack('>i', chunk2)[0],
                                  struct.unpack('>i', chunk3)[0])
                iraw = iraw + 1
                raw_index[iraw] = self.raw_nodes[struct.unpack('>i', chunk1)[0]]
                iraw = iraw + 1
                raw_index[iraw] = self.raw_nodes[struct.unpack('>i', chunk2)[0]]
                iraw = iraw + 1
                raw_index[iraw] = self.raw_nodes[struct.unpack('>i', chunk3)[0]]
        return indices, raw_index

    def load_edges(self):
        edges = {}
        with open(self.tedg, 'rb') as f:
            i = 0
            while True:
                chunk1 = f.read(4)
                chunk2 = f.read(4)
                chunk3 = f.read(4)
                if any(c == '' for c in [chunk1, chunk2, chunk3]):
                    break
                else:
                    i = i + 1
                    edges[i] = (struct.unpack('>i', chunk1)[0],
                                struct.unpack('>i', chunk2)[0],
                                struct.unpack('>i', chunk3)[0])
        edge_values = {}
        with open(self.teval, 'rb') as f:
            i = 0
            while True:
                chunk1 = f.read(4)
                chunk2 = f.read(4)
                chunk3 = f.read(4)
                chunk4 = f.read(4)
                if any(c == '' for c in [chunk1, chunk2, chunk3, chunk4]):
                    break
                else:
                    i = i + 1
                    edge_values[i] = (struct.unpack('>i', chunk1)[0],
                                      struct.unpack('>i', chunk2)[0],
                                      struct.unpack('>i', chunk3)[0])
                                      # struct.unpack('>i', chunk4)[0])
        return edges, edge_values

    def generate_triangles(self, nodes, indices):
        triangles = {}
        for tindex, nodeindices in indices.items():
            if all(nodes[i].z > 0 for i in nodeindices):
                triangles[tindex] = Polygon([(nodes[i].x, nodes[i].y) for i in nodeindices])
        return triangles

    def generate_hull_polygons(self, nodes, hull_indices):
        hull_polygon_nodes = {}
        hull_polygons = {}
        i = 0
        hull_polygon_nodes[i] = []
        for index in hull_indices[hull_indices.index(-1) + 1:]:  # Ignore Superhull
            if index == 0:
                i = i + 1
                hull_polygon_nodes[i] = []
            else:
                hull_polygon_nodes[i].append(nodes[index])
        for indexhull, hnodes in hull_polygon_nodes.items():
            hull_polygons[indexhull] = Polygon([(n.x, n.y) for n in hnodes])
        return hull_polygons

    def generate_breaklines(self):
        segments = {}
        for i, edge in self.edge_values.items():
            segments[i] = {"geometry": LineString([(self.raw_indices[edge[0]].x, self.raw_indices[edge[0]].y, self.raw_indices[edge[0]].z),
                                                   (self.raw_indices[edge[1]].x, self.raw_indices[edge[1]].y, self.raw_indices[edge[1]].z,)]),
                           "linetype": "HARD" if edge[2] == 4 else "SOFT"}

        # from shapely.ops import linemerge
        #
        # outsegs = {}
        # i2 = 0
        # for ltype in ['HARD', 'SOFT']:
        #     i2 = i2 + 1
        #     segs = [s['geometry'] for s in segments.values() if s['linetype'] == ltype]
        #     for line in list(linemerge(segs)):
        #         outsegs[i2] = {'geometry':line, 'linetype': ltype}

        return segments

    def generate_tin_nodes(self):
        from shapely.prepared import prep
        prep_hull = prep(self.hull_polygons[0])
        return dict(filter(prep_hull.covers, list(self.nodes.values())))

    def load_header_stats(self):
        stats = {}
        with open(self.tdenv, 'rb') as f:
            stats["number_total_points"] = struct.unpack('>i', f.read(4))[0]
            stats["number_triangles"] = struct.unpack('>i', f.read(4))[0]
            stats["number_indices"] = struct.unpack('>i', f.read(4))[0]
            stats["number_breaking_edge_entries"] = struct.unpack('>i', f.read(4))[0]
            stats["number_traingles_nomask"] = struct.unpack('>i', f.read(4))[0]
            stats["number_regular_points"] = struct.unpack('>i', f.read(4))[0]
            stats["number_super_points"] = struct.unpack('>i', f.read(4))[0]
            stats["min_height"] = struct.unpack('>f', f.read(4))[0]
            stats["max_height"] = struct.unpack('>f', f.read(4))[0]
            stats["unknown_1"] = struct.unpack('>i', f.read(4))[0]
            stats["extent_mix_x"] = struct.unpack('>d', f.read(8))[0]
            stats["extent_mix_y"] = struct.unpack('>d', f.read(8))[0]
            stats["extent_max_x"] = struct.unpack('>d', f.read(8))[0]
            stats["extent_max_y"] = struct.unpack('>d', f.read(8))[0]
            stats["unknown_2"] = struct.unpack('>d', f.read(8))[0]
            stats["unknown_3"] = struct.unpack('>d', f.read(8))[0]
            stats["unknown_4"] = struct.unpack('>i', f.read(4))[0]
            stats["number_tags"] = struct.unpack('<i', f.read(4))[0]
            stats["unknown_5"] = struct.unpack('>i', f.read(4))[0]
            stats["unknown_6"] = struct.unpack('>i', f.read(4))[0]
        return stats

    def load_hull(self):
        hull_index = []
        with open(self.thul, 'rb') as f:
            while True:
                chunk = f.read(4)
                if chunk == "":
                    break
                else:
                    hull_index.append(struct.unpack('>i', chunk)[0])
        return hull_index

    def load_mask(self):
        mask = {}
        with open(self.tmsk, 'rb') as f:
            mask_header = self.shp_header(f)

        return mask_header, mask

    def load_mask_index(self):
        mask_index = {}
        with open(self.tmsx, 'rb') as f:
            mask_index_header = self.shp_header(f)

        return mask_index_header, mask_index

    def load_node_info(self):
        node_info = []
        with open(self.tnodinfo, 'rb') as f:
            i = 0
            while True:
                chunk = f.read(2)
                if chunk == "":
                    break
                else:
                    i = i + 1
                    node_info.append((i,
                                      struct.unpack('>h', chunk)[0]))
        return node_info

    def load_node_tags(self):
        node_tags = []
        with open(self.tnval, 'rb') as f:
            i = 0
            while True:
                chunk = f.read(4)
                if chunk == "":
                    break
                else:
                    i = i + 1
                    node_tags.append((i,
                                      struct.unpack('<i', chunk)[0]))
        return node_tags

    def __load_projection(self):
        with open(self.prj, 'rt') as f:
            return f.readline()

    def export_shp_nodes(self, outshp):
        from osgeo import ogr

        # Now convert it to a shapefile with OGR
        driver = ogr.GetDriverByName('Esri Shapefile')
        ds = driver.CreateDataSource(outshp)
        layer = ds.CreateLayer('', None, ogr.wkbPoint25D)
        # Add one attribute
        layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        defn = layer.GetLayerDefn()

        ## If there are multiple geometries, put the "for" loop here
        for id, point in self.tin_nodes.items():
            # Create a new feature (attribute and geometry)
            feat = ogr.Feature(defn)
            feat.SetField('id', int(id))

            # Make a geometry, from Shapely object
            geom = ogr.CreateGeometryFromWkb(point.wkb)
            feat.SetGeometry(geom)

            layer.CreateFeature(feat)
            feat = geom = None  # destroy these

        # Save and close everything
        ds = layer = feat = geom = None

        return

    def export_shp_triangles(self, outshp):
        from osgeo import ogr

        # Now convert it to a shapefile with OGR
        driver = ogr.GetDriverByName('Esri Shapefile')
        ds = driver.CreateDataSource(outshp)
        layer = ds.CreateLayer('', None, ogr.wkbPolygon)
        # Add one attribute
        layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        defn = layer.GetLayerDefn()

        ## If there are multiple geometries, put the "for" loop here
        for id, poly in self.triangles.items():
            # Create a new feature (attribute and geometry)
            feat = ogr.Feature(defn)
            feat.SetField('id', int(id))

            # Make a geometry, from Shapely object
            geom = ogr.CreateGeometryFromWkb(poly.wkb)
            feat.SetGeometry(geom)

            layer.CreateFeature(feat)
            feat = geom = None  # destroy these

        # Save and close everything
        ds = layer = feat = geom = None

    def export_shp_hull(self, outshp):
        from osgeo import ogr

        # Now convert it to a shapefile with OGR
        driver = ogr.GetDriverByName('Esri Shapefile')
        ds = driver.CreateDataSource(outshp)
        layer = ds.CreateLayer('', None, ogr.wkbPolygon)
        # Add one attribute
        layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        defn = layer.GetLayerDefn()

        ## If there are multiple geometries, put the "for" loop here
        for id, poly in self.hull_polygons.items():
            # Create a new feature (attribute and geometry)
            feat = ogr.Feature(defn)
            feat.SetField('id', int(id))

            # Make a geometry, from Shapely object
            geom = ogr.CreateGeometryFromWkb(poly.wkb)
            feat.SetGeometry(geom)

            layer.CreateFeature(feat)
            feat = geom = None  # destroy these

        # Save and close everything
        ds = layer = feat = geom = None

    def export_shp_breaklines(self, outshp):
        from osgeo import ogr

        # Now convert it to a shapefile with OGR
        driver = ogr.GetDriverByName('Esri Shapefile')
        ds = driver.CreateDataSource(outshp)
        layer = ds.CreateLayer('', None, ogr.wkbLineString25D)
        # Add one attribute
        layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        fieldLineType = ogr.FieldDefn('LineType', ogr.OFTString)
        fieldLineType.SetWidth(4)
        layer.CreateField(fieldLineType)

        defn = layer.GetLayerDefn()

        ## If there are multiple geometries, put the "for" loop here
        for id, line in self.breaklines.items():
            # Create a new feature (attribute and geometry)
            feat = ogr.Feature(defn)
            feat.SetField('id', int(id))
            feat.SetField('LineType', line['linetype'])

            # Make a geometry, from Shapely object
            geom = ogr.CreateGeometryFromWkb(line['geometry'].wkb)
            feat.SetGeometry(geom)

            layer.CreateFeature(feat)
            feat = geom = None  # destroy these

        # Save and close everything
        ds = layer = feat = geom = None


    @staticmethod
    def shp_header(f):
        header = {}
        header["file_code"] = struct.unpack('>i', f.read(4))[0]
        header["field_1"] = struct.unpack('>i', f.read(4))[0]
        header["field_2"] = struct.unpack('>i', f.read(4))[0]
        header["field_3"] = struct.unpack('>i', f.read(4))[0]
        header["field_4"] = struct.unpack('>i', f.read(4))[0]
        header["field_5"] = struct.unpack('>i', f.read(4))[0]
        header["file_length"] = struct.unpack('>i', f.read(4))[0]
        header["version"] = struct.unpack('<i', f.read(4))[0]
        header["shapetype"] = struct.unpack('<i', f.read(4))[0]
        header["min_x_extent"] = struct.unpack('<d', f.read(8))[0]
        header["min_y_extent"] = struct.unpack('<d', f.read(8))[0]
        header["max_x_extent"] = struct.unpack('<d', f.read(8))[0]
        header["max_y_extent"] = struct.unpack('<d', f.read(8))[0]
        header["min_z_extent"] = struct.unpack('<d', f.read(8))[0]
        header["max_z_extent"] = struct.unpack('<d', f.read(8))[0]
        header["min_m_extent"] = struct.unpack('<d', f.read(8))[0]
        header["max_m_extent"] = struct.unpack('<d', f.read(8))[0]

        return header
