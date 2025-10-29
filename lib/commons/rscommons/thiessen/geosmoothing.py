# Borrowed from:
# https://github.com/GeographicaGS/GeoSmoothing/blob/master/geosmoothing/geosmoothing.py
# -*- coding: utf-8 -*-
#
#  Author: Cayetano Benavent, 2016.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#

import numpy as np
from rsxml import Logger
from scipy.interpolate import splprep, splev
from shapely.geometry import LineString, Polygon, mapping, asShape, MultiPolygon


class GeoSmtBase(object):

    def __init__(self):
        self.log = Logger("GeoSmtBase")


class Splines(GeoSmtBase):

    def __init__(self):
        self.lg = GeoSmtBase()
        self.log = Logger('Splines')

    def compSplineKnots(self, x, y, s, k, nest=-1):
        """
        Computed with Scipy splprep. Find the B-spline representation of
        an N-dimensional curve.
        Spline parameters:
        :s - smoothness parameter
        :k - spline order
        :nest - estimate of number of knots needed (-1 = maximal)
        """

        tck_u, fp, ier, msg = splprep([x, y], s=s, k=k, nest=nest, full_output=1)

        if ier > 0:
            self.log.error("{0}. ier={1}".format(msg, ier))

        return (tck_u, fp)

    def compSplineEv(self, x, tck, zoom=10):
        """
        Computed with Scipy splev. Given the knots and coefficients of
        a B-spline representation, evaluate the value of the smoothing
        polynomial and its derivatives
        Parameters:
        :tck - A tuple (t,c,k) containing the vector of knots,
             the B-spline coefficients, and the degree of the spline.
        """

        n_coords = len(x)
        n_len = n_coords * zoom
        x_ip, y_ip = splev(np.linspace(0, 1, n_len), tck)

        return (x_ip, y_ip)


class GeoSmoothing(GeoSmtBase):

    def __init__(self, spl_smpar=0, spl_order=2):
        """
        spl_smpar: smoothness parameter
        spl_order: spline order
        """
        self.__spl_smpar = spl_smpar
        self.__spl_order = spl_order

        self.lg = GeoSmtBase()

        self.log = Logger('GeoSmoothing')

    def __getCoordinates(self, geom):
        """
        Getting x,y coordinates from geometry...
        """
        if isinstance(geom, LineString):
            x = np.array(geom.coords.xy[0])
            y = np.array(geom.coords.xy[1])

        elif isinstance(geom, Polygon):
            x = np.array(geom.exterior.coords.xy[0])
            y = np.array(geom.exterior.coords.xy[1])

        return (x, y)

    def __getGeomIp(self, coords_ip, geom):
        """
        """
        if isinstance(geom, LineString):
            geom_ip = LineString(coords_ip.T)

        elif isinstance(geom, Polygon):
            geom_ip = Polygon(coords_ip.T)

        return geom_ip

    def smooth(self, geom):
        if isinstance(geom, LineString):
            return self.__smoothGeom(geom)

        elif isinstance(geom, Polygon):
            ext = self.__smoothGeom(Polygon(geom.exterior))
            if geom.interiors > 0:
                newInteriors = []
                for interior in geom.interiors:
                    newInteriors.append(self.__smoothGeom(Polygon(interior)))

        return ext.difference(MultiPolygon(newInteriors))

    def __smoothGeom(self, geom):
        """
        Run smoothing geometries
        """
        x, y = self.__getCoordinates(geom)

        spl = Splines()

        tck_u, _fp = spl.compSplineKnots(x, y, self.__spl_smpar, self.__spl_order)
        x_ip, y_ip = spl.compSplineEv(x, tck_u[0])

        coords_ip = np.array([x_ip, y_ip])

        return self.__getGeomIp(coords_ip, geom)
