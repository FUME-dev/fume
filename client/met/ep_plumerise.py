"""
Description: Functions:
  plumerise: calculates plume rise
  get_plume_frac: calculates plume depth and factors for each layer for how much of the emissions it receives
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

Copyright 2014-2023 Institute of Computer Science of the Czech Academy of Sciences, Prague, Czech Republic
Copyright 2014-2023 Charles University, Faculty of Mathematics and Physics, Prague, Czech Republic
Copyright 2014-2023 Czech Hydrometeorological Institute, Prague, Czech Republic
Copyright 2014-2017 Czech Technical University in Prague, Czech Republic
"""

import numpy as np

p0 = 1000.
gamma = 0.286
grav = 9.81


def plumerise(nlay, hght,temp,press,wind,hstk,dstk,tstk,vstk):
    """
    As in CAMx v6.30 20160408

    PLUMERIS calculates plume rise above a given elevated point source.
    Methodology based on the EPA TUPOS Gaussian Plume Model
    (Turner, Chico, and Catalano, 1986)

    Modifications: 
       04/07/10       Various updates to improve robustness

    Input arguments:
       nlay                number of layers
       hght                layer interface heights (m)
       temp                temperature (K)
       press               pressure (hPa = mb)
       wind                total horizontal wind (m/s)
       hstk                stack height (m)
       dstk                stack diameter (m)
       tstk                stack temperature (K)
       vstk                stack velocity (m/s)

    Output arguments:
       prise               plume rise (m)
    """

    lfirst = True
    rise = 0.
    dwfact = 1.
    hstk = max(hstk,1.)
    hstk = min(hstk,hght[nlay-1])
    dstk = max(dstk,0.1)
    vstk = max(vstk,0.1)

    dtdz = np.zeros(nlay+1)
# Calculate potential temperature lapse rate
    for k in range(1, nlay):
        if ( k < nlay ):
            dz = hght[k+1]/2.
            if ( k > 1 ):
                dz = (hght[k+1] -  hght[k-1])/2.
            dtheta = temp[k+1]*(p0/press[k+1])**gamma - temp[k]*(p0/press[k])**gamma
            dtdz[k] = dtheta/dz
        else:
            dtdz[k] = dtdz[k-1]

# Find beginning layer; determine vertical coordinates relative
#     to stack-top 

    kstk = nlay
    for k in range(1, nlay-1):
        if (hstk < hght[k]):
            kstk = k # we found the model layer corresponding to the stack
            break
    if kstk == nlay: # the stack taller than the model highest level
        prise = hstk
        return prise
    
    ztop = hght[kstk] - hstk
    zbot = 0.
    zstab = 0.
    stkt = max(tstk,temp[kstk] + 1.)

# Determine downwash factor as a function of stack Froude number

    fr = temp[kstk]*vstk*vstk/(grav*dstk*(stkt - temp[kstk]))
    if ( fr  >= 3. ):
        if ( wind[kstk] >= vstk):
            prise = hstk
            return prise
        elif (wind[kstk] >= vstk/1.5 and wind[kstk] < vstk):
            dwfact = 3.*(vstk - wind[kstk])/vstk
#minimum windspeed profile: 1 m/s
    for k  in range(1,nlay):
        wind[k] = max(wind[k],1.)

# Neutral-unstable conditions for momentum rise and stack buoyancy flux

    umrise = 3.*dstk*vstk/wind[kstk]
    if (umrise > ztop):
        wsum = wind[kstk]*ztop
        for k in range(kstk+1,nlay):
            wsum = wsum + wind[k]*(hght[k] - hght[k-1])
            wavg = wsum/(hght[k] - hstk)
            umrise = 3.*dstk*vstk/wavg
            if (umrise < ght[k]-hstk):
                break
        
    bflux0 = grav*vstk*dstk*dstk*(stkt - temp[kstk])/(4.*stkt)
    bflux = bflux0

#  Top of layer loop; determine stability

    while True:  
        if (lfirst):
            kstab = kstk
            if (kstk > 1 and hstk < ((hght[kstk]+hght[kstk-1])/2.)):
                kstab = kstk - 1
        else:
            kstab = kstk-1
            bflux = rflux
            ztop = hght[kstk] - hstk
            zbot = hght[kstk-1] - hstk
        if (dtdz[kstab] > 1.5e-3):
            if (zstab  == 0.):
                zstab = max(zbot,1.)
        else:
            ubrise1 = 30.*(bflux/wind[kstk])**0.6 + zbot
            ubrise2 = 24.*(bflux/wind[kstk]**3)**0.6 * (hstk + 200.*(bflux/wind[kstk]**3))**0.4 + zbot
            ubrise = min(ubrise1,ubrise2)
            iuse = 1
            if (ubrise == ubrise2):
                 iuse = 2

# Find the maximum of neutral-unstable momentum and buoyancy

            if (lfirst and umrise > ubrise):
                rise = umrise
                prise = hstk + dwfact*rise
                return prise
            else:
                rise = ubrise
                if (rise <= ztop):
                    prise = hstk + dwfact*rise
                    return prise        

# Neutral-unstable residual buoyancy flux 
            if (iuse == 1):
                rflux = wind[kstk]*((rise - ztop)/30.)**(5./3.)
            else:
                rflux = 0.0055*(rise - ztop)*wind[kstk]**3 /(1. + hstk/(rise - ztop))**(2./3.)
      
            kstk = kstk + 1
            if (kstk > nlay or rflux <= 0.):
                prise = hstk + dwfact*rise 
                return prise

            lfirst = False

# Stable buoyancy rise

        sbrise1 = (1.8*bflux*temp[kstk]/(wind[kstk]*dtdz[kstab]) +  zbot*zbot*zbot)**(1./3.)
        sbrise2 = (4.1*bflux*temp[kstk]/(bflux0**(1./3.)*dtdz[kstab]) + zbot**(8./3.))**(3./8.)
        sbrise = min(sbrise1,sbrise2)
        iuse = 1
        if (sbrise == sbrise2):
            iuse = 2

# Stable momentum rise
        if (lfirst):
            smrise = 0.646*(vstk**2*dstk**2/(stkt*wind[kstk]))**(1./3.) * sqrt(temp[kstk])/(dtdz[kstab]**(1./6.))
            if (smrise > ztop):
                wsum = wind[kstk]*ztop
                tsum = temp[kstk]*ztop
                ssum = dtdz[kstab]*ztop
                for k in range(kstk+1,nlay):
                    wsum = wsum + wind[k]*(hght[k] - hght[k-1])
                    wavg = wsum/(hght[k] - hstk)
                    tsum = tsum + temp[k]*(hght[k] - hght[k-1])
                    tavg = tsum/(hght[k] - hstk)
                    ssum = ssum + dtdz[k-1]*(hght[k] - hght[k-1])
                    savg = ssum/(hght[k] - hstk)
                    smrise = 0.646*(vstk**2*dstk**2/(stkt*wavg))**(1./3.) *  sqrt(tavg)/(savg**(1./6.))
                    if (smrise < hght[k]-hstk):
                        break
            smrise = min(smrise,umrise)

# Find maximum between stable momentum and (2/3)*bouyancy rise

            if (smrise < (2.*sbrise/3.)):
                rise = smrise
                prise = hstk + dwfact*rise
                return prise

        if (sbrise <= ztop):
            rise = zstab + 2.*(sbrise - zstab)/3.
            prise = hstk + dwfact*rise
            return prise
        else:
            rise = sbrise

# Stable residual buoyancy flux

        if ( iuse == 1 ):
            rflux = bflux - 0.56*dtdz[kstab]*wind[kstk]/temp[kstk]* (ztop*ztop*ztop - zbot*zbot*zbot)
        else:
            rflux = bflux - 0.24*dtdz[kstab]*bflux0**(1./3.)/temp[kstk]* (ztop**(8./3.) - zbot**(8./3.))

        kstk = kstk + 1
        if (kstk > nlay or rflux <= 0.):
            rise = zstab + 2.*(sbrise - zstab)/3.
            prise = hstk + dwfact*rise
            return prise
   
        lfirst = False

    prise = hstk + dwfact*rise

    return prise


def get_plume_frac(nlay, hght,temp,press,wind,hstk,dstk,tstk,vstk):
    """
    Calculate the fraction of emission each layer receives.
    """
    pfrac = np.zeros(nlay+1,dtype = np.float )
    prise = plumerise(nlay,hght,temp,press,wind,hstk,dstk,tstk,vstk,zstk)
    k = 1
    layerfound = False
    if (hght[nlay] <= zstk):
        kstk = nlay
        zstk = hght[kstk-1] + 1.
        layerfound = True

    while (k <= nlay and layerfound == False):
        if (hght(k) > zstk):
            layerfound = True
        else:
            k =+ 1
    kstk = k            
    
    wp = max(1.,vstk/2.)
    tp = (temp[kstk] + tstk)/2.
    zrise = zstk - hstk
    trise = zrise/wp
    pwidth = sqrt(2.)*dstk
    rip = grav*pwidth*abs(tp - temp[kstk])/(temp[kstk]*wp*wp)
    fp = 1. + 4.*rip
    qp2 = fp*wp*wp*(cq1 + cq2*wind[kstk]*wind[kstk]/(wind[kstk]*wind[kstk] + wp*wp))
    rkp = 0.15*pwidth*sqrt(qp2)
    pwidth = 3.*sqrt(pwidth*pwidth + 2.*rkp*trise)
    pwidth = max(1.,min(pwidth,zrise))
    zbot = max(0.,zstk - pwidth/2.)
    ztop = min(hght[nlay],zstk + pwidth/2.)

# Calculate layers to receive emissions

    pwidth = ztop - zbot

    kt = nlay
    for kt in range(kstk,nlay):
        if (hght[kt] >= ztop):
            break
    for kb in range(1,kstk):
        if  (hght[kb] > zbot):    
            break
    for k in  range(kb,kt):
        bot = max(hght[k-1],zbot)
        top = min(hght[k],ztop)
        pfrac[k] = (top - bot)/pwidth

    return pfrac
