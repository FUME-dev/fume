from datetime import datetime, timedelta
import numpy as np
import lib.ep_io_fortran as mt
from postproc.receiver import DataReceiver, requires


gdtype_mapping = {
        'LATLON': 0,
        'UTM': 1,
        'LAMBERT': 2,
        'STEREO': 3,
        'POLAR': 4,
        'EMERCATOR': 5,
        'MERCATOR': 5
}
ione=int(1)
rdum=float(0.)
def camx_date_time(dt):
    return ((dt.year*1000 + dt.timetuple().tm_yday)%100000,
            dt.hour + (dt.minute + dt.second/60.0)/60.0)


def long_species_name(s):
    return '{0:16s}'.format(s)


class CAMxWriter(DataReceiver):
    """
    Postprocessor class for writing CAMx emission input files.
    This is the base class for the common CAMx format settings and should not be
    instantiated by itself. Descendant classes CAMxAreaWriter or CAMxPointWriter
    should be used instead.

    Implements only common setup (create output netcdf file, time variable) and
    finalize (set common global attributes of the netcdf file and close the
    file).
    """

    def setup(self, filename):

        self.outfile = open(filename,mode='wb')
        
        # datetime data needed when writing both file header and data for both CAMx point and area sources
        bdate, btime = [],[]
        for dt in self.rt_cfg['run']['datestimes']:
            dt1, dt2 = camx_date_time(dt)
            bdate.append(dt1)
            btime.append(dt2)        
        # for CAMx, additional timestep is needed
        
        edatetime  = self.rt_cfg['run']['datestimes'][-1]
        dt_p1 = edatetime+timedelta(seconds=self.cfg.run_params.time_params.timestep)
  
        dt1, dt2 = camx_date_time(dt_p1)
        bdate.append(dt1)
        btime.append(dt2)
        
        self.bdate = bdate
        self.btime = btime
        
    def finalize(self):
        
        self.outfile.close()


class CAMxAreaWriter(CAMxWriter):
    """
    Postprocessor class for writing CAMx area emission file.
    """

    def setup(self):
        super().setup(self.cfg.postproc.camxareawriter.outfile)



    def receive_area_species(self, species):
        
        self.species = species
        self.numspec = len(species)
        
        emiss = 'EMISSIONS '
        emisslong = [ emiss[i]+'   ' for i in range(10)  ]
        
        notes = 'CAMx area emissions created by EP'
        if len(notes)>60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)
        notesformatted = [ tmp[i]+'   ' for i in  range(60) ]
        emisname = [ '{:10s}'.format(species[i][1]) for i in range(self.numspec)  ]
              
        longemisname = ['' for i in range(self.numspec)]
        for i in range(self.numspec):
            longemisname[i] = [emisname[i][j]+'   '  for j in range(10) ]
            
        self.longemisname = longemisname

        
        istag = 0
        
        
        
        proj  = self.rt_cfg['projection_params']['proj']
        p_alp = self.rt_cfg['projection_params']['p_alp']
        p_bet = self.rt_cfg['projection_params']['p_bet']
        p_gam = self.rt_cfg['projection_params']['p_gam']
        XCENT = self.rt_cfg['projection_params']['lon_central']
        YCENT = self.rt_cfg['projection_params']['lat_central']
        
        # domain parameters
        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz
        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely
        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg
        # the S/W corner of the grid (as gridboxes)
        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0
        endian = self.cfg.run_params.output_params.endian
        
        mt.write_record (self.outfile, endian, '40s240siiifif',''.join(emisslong).encode('utf-8'),''.join(notesformatted).encode('utf-8'),int(self.cfg.run_params.time_params.itzone_out),int(self.numspec),self.bdate[0],self.btime[0],self.bdate[-1],self.btime[-1] )
        mt.write_record (self.outfile, self.cfg.run_params.output_params.endian, 'ffiffffiiiiifff', XCENT, YCENT,int(p_alp),xorig,yorig,delx,dely,nx,ny,nz,int(gdtype_mapping[proj]),istag,p_alp,p_bet,rdum)
        mt.write_record (self.outfile, endian, 'iiii', ione,ione,nx,ny)
        
        joinedstr = [''.join(longemisname[i]) for i in range(self.numspec)]
       
        fmt_str = str(40*self.numspec)+'s'
        mt.write_record (self.outfile, endian, fmt_str, ''.join(joinedstr).encode('utf-8') )
 
 

    def receive_area_emiss(self, timestep, data):

        mt.write_record(self.outfile, self.cfg.run_params.output_params.endian, 'ifif', self.bdate[timestep],self.btime[timestep],self.bdate[timestep+1],self.btime[timestep+1])
        for i in range(self.numspec):
            fmt_str = 'i40s'+str(self.cfg.domain.nx*self.cfg.domain.ny)+'f'
            emis2d = np.sum(data, axis=2) # in CAMx, we do not have elevated emissions (3D emissions), so sum up to the ground
            data2d = emis2d[:,:,i].flatten('F')*self.cfg.run_params.time_params.timestep
            mt.write_record(self.outfile, self.cfg.run_params.output_params.endian, fmt_str, ione, ''.join(self.longemisname[i]).encode('utf-8'), *data2d)

        
    def finalize(self):
        
        super().finalize()

class CAMxPointWriter(CAMxWriter):
    """
    Postprocessor class for writing CAMx point emission file.
    """

    def setup(self):

       super().setup(self.cfg.postproc.camxpointwriter.outfile)

                          

    def receive_stack_params(self, stacks):
        self.point_src_params = np.array(stacks, dtype = np.float)
        self.numstk = self.point_src_params.shape[0]
        self.stacks_id = list(map(int,self.point_src_params[:,0]))



    @requires('stack_params')
    def receive_point_species(self, species):
        

        self.species = species
        self.numspec = len(species)
        
        emiss = 'PTSOURCE  '
        emisslong = [ emiss[i]+'   ' for i in range(10)  ]
        
        notes = 'CAMx point emissions created by EP'
        if len(notes)>60:
            tmp = notes[0:60]
        else:
            tmp = '{:60s}'.format(notes)
        notesformatted = [ tmp[i]+'   ' for i in  range(60) ]
        
        emisname = [ '{:10s}'.format(self.species[i][1]) for i in range(self.numspec)]
        
        longemisname = ['' for i in range(self.numspec)]
        
        for i in range(self.numspec):
            longemisname[i] = [emisname[i][j]+'   '  for j in range(10) ]
            
        self.longemisname = longemisname
        
        istag = 0
        
        
        
        proj  = self.rt_cfg['projection_params']['proj']
        p_alp = self.rt_cfg['projection_params']['p_alp']
        p_bet = self.rt_cfg['projection_params']['p_bet']
        p_gam = self.rt_cfg['projection_params']['p_gam']
        XCENT = self.rt_cfg['projection_params']['lon_central']
        YCENT = self.rt_cfg['projection_params']['lat_central']
               # domain parameters
        nx = self.cfg.domain.nx
        ny = self.cfg.domain.ny
        nz = self.cfg.domain.nz
        delx = self.cfg.domain.delx
        dely = self.cfg.domain.dely
        xorg = self.cfg.domain.xorg
        yorg = self.cfg.domain.yorg
        # the S/W corner of the grid (as gridboxes)
        xorig = xorg - nx*delx/2.0
        yorig = yorg - ny*dely/2.0
        endian = self.cfg.run_params.output_params.endian
        
        mt.write_record (self.outfile, endian, '40s240siiifif',''.join(emisslong).encode('utf-8'),''.join(notesformatted).encode('utf-8'),int(self.cfg.run_params.time_params.itzone_out),int(self.numspec),self.bdate[0],self.btime[0],self.bdate[-1],self.btime[-1] )
        mt.write_record (self.outfile, self.cfg.run_params.output_params.endian, 'ffiffffiiiiifff', XCENT, YCENT,int(p_alp),xorig,yorig,delx,dely,nx,ny,nz,int(gdtype_mapping[proj]),istag,p_alp,p_bet,rdum)
        mt.write_record (self.outfile, endian, 'iiii', ione,ione,nx,ny)
        
        joinedstr = [''.join(longemisname[i]) for i in range(self.numspec)]
       
        fmt_str = str(40*self.numspec)+'s'
        mt.write_record (self.outfile, endian, fmt_str, ''.join(joinedstr).encode('utf-8') )

        mt.write_record(self.outfile, endian, 'ii', ione, self.numstk)

        var_list = []
        for i in range(self.numstk):
            stk_list = [ self.point_src_params[i,2], self.point_src_params[i,4],  self.point_src_params[i,5],  self.point_src_params[i,6],  self.point_src_params[i,7],  self.point_src_params[i,8] ]
            var_list.extend(stk_list)

        fmt_str = str(6*self.numstk)+'f'
        mt.write_record(self.outfile, endian, fmt_str, *var_list)


      


    @requires('point_species')
    def receive_point_emiss(self, timestep, data):
        endian = self.cfg.run_params.output_params.endian
        mt.write_record(self.outfile, endian, 'ifif', self.bdate[timestep],self.btime[timestep],self.bdate[timestep+1],self.btime[timestep+1] )
        mt.write_record(self.outfile, endian, 'ii', ione, self.numstk )
        var_list = []
        # create list to write
        
        for i in range(self.numstk):
            var_list.extend([ ione, ione, ione, rdum, rdum ] )
        fmt_str = self.numstk*'iiiff'

        mt.write_record(self.outfile, endian, fmt_str, *var_list )

       
        pemis = np.array(data)
        for i in range(self.numspec):
            pemis1 = pemis[:,i]*self.cfg.run_params.time_params.timestep
            fmt_str = 'i40s'+str(self.numstk)+'f'
            mt.write_record(self.outfile, endian, fmt_str, ione, ''.join(self.longemisname[i]).encode('utf-8') , *pemis1)
  



        
    def finalize(self):
        



        # finalize the point emission file as well
        super().finalize()

         
