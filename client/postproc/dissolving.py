"""
Description: FUME module for dissolving of emissions.
"""

"""
This file is part of the FUME emission model.

FUME is free software: you can redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

FUME is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

Information and source code can be obtained at www.fume-ep.org

Copyright 2024-2026 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
"""

import re
import numpy as np
from numpy.linalg import solve
from lib.ep_config import ep_cfg
import lib.ep_logging
log = lib.ep_logging.Logger(__name__)

na_ = np.newaxis

axnames = 'zyx'
axes = {a:i for i,a in enumerate(axnames)}
dis_order_re = re.compile(r'\s*([-\+]?)(\d+)([' + axnames + r'])\s*')

class BadDissovlingOrder(Exception):
    pass

class DissolvingOrder:
    def __init__(self, order):
        """Parses the dissolving order string from config"""

        self.steps = [] #(axis, nplus, nminus) for each specified dissolving step
        self.expansion = [[0,0] for _ in axnames]

        # Keep parsing order util empty
        while order:
            m = dis_order_re.match(order)
            if m is None:
                raise BadDissovlingOrder('Unable to parse dissolving order: ' + order)

            sign, length, axis = m.groups()
            nplus = nminus = int(length)
            axis = axes[axis]

            if sign == '+':
                nminus = 0
            elif sign == '-':
                nplus = 0

            self.expansion[axis][0] = max(self.expansion[axis][0], nminus)
            self.expansion[axis][1] = max(self.expansion[axis][1], nplus)

            self.steps.append((axis, nplus, nminus))

            # Remove parsed part
            order = order[m.end():]

    def expand(self, data, pos):
        """Expand a 3D-cut in domain by the dissolving steps"""

        # Calc expanded position and dimensions (respecting domain limits). Expanded array dims are +1 greater
        # in all directions in order to preserve differences from zero-emission neighbors.
        z0, y0, x0 = pos
        zs, ys, xs = data.shape

        self.z0 = max(0, z0 - self.expansion[0][0] - 1)
        self.y0 = max(0, y0 - self.expansion[1][0] - 1)
        self.x0 = max(0, x0 - self.expansion[2][0] - 1)

        self.z1 = min(ep_cfg.domain.nz, z0 + zs + self.expansion[0][1] + 1)
        self.y1 = min(ep_cfg.domain.ny, y0 + ys + self.expansion[1][1] + 1)
        self.x1 = min(ep_cfg.domain.nx, x0 + xs + self.expansion[2][1] + 1)

        self.zs = self.z1 - self.z0
        self.ys = self.y1 - self.y0
        self.xs = self.x1 - self.x0

        # Expand data
        exp_data = np.zeros((self.zs, self.ys, self.xs), dtype=data.dtype)
        exp_data[z0-self.z0:z0-self.z0+zs,
                 y0-self.y0:y0-self.y0+ys,
                 x0-self.x0:x0-self.x0+xs] = data

        return exp_data

def dissolve(emis, hardmask, axis, nplus, nminus):
    """Perform dissolving on a given axis with specified limits"""

    coef = ep_cfg.postproc.palmwriter.dissolving.coefficient

    nax = len(emis.shape)
    mask = emis > 0.
    nemis = mask.sum()

    # Extend mask by given number of points on the selected axis
    ext_mask = mask.copy()

    for i in range(1, nplus+1):
        idx_from = [slice(None)] * nax
        idx_to   = [slice(None)] * nax
        idx_from[axis] = slice(None, -i)
        idx_to[axis]   = slice(i, None)
        ext_mask[tuple(idx_to)] |= mask[tuple(idx_from)]

    for i in range(1, nminus+1):
        idx_from = [slice(None)] * nax
        idx_to   = [slice(None)] * nax
        idx_from[axis] = slice(i, None)
        idx_to[axis]   = slice(None, -i)
        ext_mask[tuple(idx_to)] |= mask[tuple(idx_from)]

    nexp = ext_mask.sum()

    # Reduce mask using hardmask
    ext_mask &= hardmask

    log.fmt_tracing('Dissolving along {}-axis: {} emission points, {} expanded, {} hardmasked.',
            axnames[axis], nemis, nexp, ext_mask.sum())

    # Iterate over remaining axes
    other_axes = list(emis.shape)
    del other_axes[axis]
    ### other_axnames = list(axnames)
    ### del other_axnames[axis]

    for idx in np.ndindex(*other_axes):
        ### log.debug('Dissolving for', ', '.join('{}={}'.format(n, v)
        ###                                       for v, n in zip(idx, other_axnames)))

        # Select proper vector
        full_idx = list(idx)
        full_idx.insert(axis, slice(None))
        full_idx = tuple(full_idx)

        # Perform actual dissolving
        dmask = ext_mask[full_idx]
        if dmask.any():
            demis = emis[full_idx]
            dissolve1d(demis, dmask, coef)

    log.fmt_tracing('Dissolved from {} to {} points.', nemis, (emis > 0.).sum())

def dissolve1d(emis, mask, coef):
    """Perform actual 1-dimensional dissolving by optimization."""

    pcoef1 = np.array([-2., 4.+coef, -2.])
    pcoef0 = np.array([1., -2., 1.])

    # Prepare coordinates
    ne, = emis.shape
    nd = ne - 1 # number of spatial differenes
    xmask = mask[:-1] & mask[1:] # mask of possible shifts

    sdif = emis[1:] - emis[:-1]

    # Prepare system of linear eqns for solving
    coef = np.zeros((nd+2,nd+2), dtype='f8')
    ycoord0 = np.arange(0,nd)[:,na_]
    ycoord1 = np.arange(1,nd+1)[:,na_]
    ycoord2 = np.arange(2,nd+2)[:,na_]
    xcoord = np.arange(3)[na_,:] + ycoord0

    coef[ycoord1, xcoord] += pcoef1
    coef[ycoord0, xcoord] += pcoef0
    coef[ycoord2, xcoord] += pcoef0

    coefx = coef[1:-1,1:-1][xmask][:,xmask]

    # Applying coefficients [1., -2., 1.]
    const = sdif[:] * (-2.)
    const[:-1] += sdif[1:]
    const[1:] += sdif[:-1]
    constx = const[xmask]

    # Solve linear system
    shiftx = solve(coefx, constx)

    # Apply solved shifts into full arrays
    shift = np.zeros((nd,), dtype='f8')
    shift[xmask] = shiftx

    ### emis_out = emis.copy()
    ### emis_out[1:] += shift[:]
    ### emis_out[:-1] -= shift[:]

    emis[1:] += shift[:]
    emis[:-1] -= shift[:]

    ### # Verify minimum
    ### if __debug__:
    ###     base_err = ((emis_out[1:]-emis_out[:-1])**2).sum() + coef*(shift**2).sum()
    ###     print('Base error:', base_err)
    ###     for pert in np.linspace(0., 0.001, 5):
    ###         shiftx_pert = np.random.normal(shiftx, pert)
    ###         shift_pert = np.zeros((nd,), dtype='f8')
    ###         shift_pert[xmask] = shiftx_pert

    ###         emis2 = emis.copy()
    ###         emis2[1:] += shift_pert[:]
    ###         emis2[:-1] -= shift_pert[:]
    ###         print('Pert', pert, ':', ((emis2[1:]-emis2[:-1])**2).sum() + coef*(shift_pert**2).sum() - base_err)
