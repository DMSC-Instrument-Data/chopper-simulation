# -*- coding: utf-8 -*-
# *********************************************************************
# plankton - a library for creating hardware device simulators
# Copyright (C) 2016 European Spallation Source ERIC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# *********************************************************************


class Bearings(object):
    def __init__(self):
        pass

    def disengage(self):
        pass

    def engage(self):
        pass


class MagneticBearings(Bearings):
    def __init__(self):
        super(MagneticBearings, self).__init__()


class MechanicalBearings(Bearings):
    def __init__(self):
        super(MechanicalBearings, self).__init__()
