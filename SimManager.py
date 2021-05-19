#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 11:58:05 2021

@author: jguterl
"""

#import libconf
#import os,io
#import shutil, itertools
import numpy as np
import datetime
import select
import subprocess
#import threading
import curses
import multiprocessing
import time
import os
from functools import reduce
import itertools
import shutil
from Slurm import sbatch
try:
    import pyslurm
except:
    pass

#from multiprocessing import Process

class SimulationManager():
    Verbose = False
    CurrentSimu = []
    NSimu = 0
    _killBatchProc = False
    BatchProc = None

    @classmethod
    def Init(cls):
        cls.StopBatch()
        cls.CurrentSimu = []
        cls.NSimu = 0

    @classmethod
    def BatchRun(cls, Command=None, BatchSize=multiprocessing.cpu_count(), TimeOut=3, MaxTime=604800):
        print('Running batch of GITR simulations. Batchsize={}. TimeOut={}'.format(
            BatchSize, TimeOut))
        assert BatchSize > 0, "BatchSize<1"
        Tstart = time.time()
        TimeElapsed = 0
        cls.Screen = curses.initscr()
        cls.Screen.resize(len(cls.CurrentSimu)+10, 150)
        curses.resizeterm(len(cls.CurrentSimu)+10, 150)
        cls.Nprocess = 0
        while TimeElapsed < MaxTime and cls.Nprocess <= len(cls.CurrentSimu):
            TimeElapsed = time.time()-Tstart
            while cls.CheckRunningSim() < BatchSize and cls.Nprocess < len(cls.CurrentSimu):
                for S in cls.CurrentSimu:
                    if S.Process is None:
                        #print('Time elapsed: {:.1f} - Running simulations: {}'.format(TimeElapsed, cls.CheckRunningSim()))
                        S.Start(Command)
                        cls.Nprocess = cls.Nprocess + 1
                        break

            cls.Screen.clear()
            cls.Screen.addstr(0, 0, cls.DisplayOutput(TimeElapsed=TimeElapsed), curses.A_PROTECT)
            cls.Screen.refresh()
            if cls.CheckRunningSim() == 0:
                break

            time.sleep(TimeOut)

        #curses.endwin()
        cls.Screen.clear()
        cls.Screen.addstr(0, 0, cls.DisplayOutput(TimeElapsed=TimeElapsed), curses.A_PROTECT)
        cls.Screen.refresh()
        cls.StopBatch()
        
    @classmethod
    def SbatchRun(cls, Command=None, TimeOut=3, Display=True, **kwargs):
        print('Submitting slurm jobs for GITR simulations')
        Tstart = time.time()
        cls.Nprocess = len(cls.CurrentSimu)
        TimeElapsed = 0
        for S in cls.CurrentSimu:
            S.StartSlurm(Command, **kwargs)
        cls.Screen = curses.initscr()
        cls.Screen.resize(len(cls.CurrentSimu)+10, 150)
        curses.resizeterm(len(cls.CurrentSimu)+10, 150)
        cls.Screen.clear()
        cls.Screen.addstr(0, 0, cls.DisplayOutput(TimeElapsed=TimeElapsed), curses.A_PROTECT)
        cls.Screen.refresh()
        while True and Display:
            TimeElapsed = time.time()-Tstart
            cls.Screen.clear()
            cls.Screen.addstr(0, 0, cls.DisplayOutput(TimeElapsed=TimeElapsed), curses.A_PROTECT)
            cls.Screen.refresh()
            time.sleep(TimeOut)

    @classmethod
    def WatchJobs(cls):
        cls.Screen = curses.initscr()
        cls.Screen.resize(len(cls.CurrentSimu)+10, 150)
        curses.resizeterm(len(cls.CurrentSimu)+10, 150)
        cls.Screen.clear()
        cls.Screen.addstr(0, 0, cls.DisplayOutput(0), curses.A_PROTECT)
        cls.Screen.refresh()
        
    @classmethod
    def StopBatch(cls):
        for S in cls.CurrentSimu:
            S.Stop()
    def CancelSbatch(cls):
        for S in cls.CurrentSimu:
            S.Stop()

    def LaunchBatch(self, *args, **kwargs):
        self.__class__.BatchRun(*args, **kwargs)
    
    def LaunchSbatch(self, *args, **kwargs):
        self.__class__.SbatchRun(*args, **kwargs)

    @classmethod
    def CheckRunningSim(cls):
        N = 0
        for S in cls.CurrentSimu:
            try:
                if S.Process.poll() is None:
                    N = N + 1
            except:
                pass
        return N

    @classmethod
    def StatusBatchProc(cls):
        try:
            return cls.BatchProc.is_alive()
        except:
            return False

    @classmethod
    def DisplayOutput(cls, TimeElapsed=0):
        LineWidth = 120
        Sout = []
        Sout.append("|{0: <{width}.{width}}|".format(
            '-'*LineWidth, width=LineWidth))
        Sout.append("|{0: <{width}.{width}}|".format(
            'Simulations status - Time elapsed:{:.1f} - Runs:{}/{}'.format(TimeElapsed,cls.Nprocess, len(cls.CurrentSimu)), width=LineWidth))
        Sout.append("|{0:<{width}.{width}}|".format(
            '-'*LineWidth, width=LineWidth))
        S = '{0: ^5.5s} | {1: ^6.6s} | {2: <50.50s} | {3: <50.50s}'.format(
            '#', 'Status', 'Directory', 'Output')
        Sout.append("|{0: <{width}.{width}}|".format(S, width=LineWidth))
        Sout.append("|{0: <{width}.{width}}|".format(
            '-'*LineWidth, width=LineWidth))
        for S in cls.CurrentSimu:
            S = '{0:^5} | {1:^6.6s} | {2: <50s} | {3: <50s}'.format(
                S.Id, str(S.Status), S.Directory.split('/')[-1], S.LastLine())
            Sout.append("|{0: <{width}.{width}}|".format(S, width=LineWidth))
        Sout.append("|{0: <{width}.{width}}|".format(
            '-'*LineWidth, width=LineWidth))
        Sout = '\n'.join(Sout)

        return Sout

    def AddSimulation(self,*args,**kwargs):
        if kwargs.get('SimulationManager') is None:
            kwargs['SimulationManager'] = self.__class__
        Simulation(*args,**kwargs)


    @classmethod
    def _DumpInfo(cls,FileName=None, Folder=None):

        if FileName is None:
            FileName='log_{}'.format(datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S'))
        if Folder is not None:
            FilePath = os.path.join(Folder, FileName)
        else:
            FilePath = FileName

        print('Logging simulation details into {}'.format(FilePath))
        cls.LogPath = FilePath
        np.save(os.path.splitext(FilePath)[0]+'.npy',np.array(cls.CurrentSimu))
        np.save(os.path.join(Folder, 'last.npy'),np.array(cls.CurrentSimu))

    def DumpInfo(self,*args,**kwargs):
        self.__class__._DumpInfo( *args, **kwargs)

    def SetSimulation(self,SimInfo:dict, Directory: str, Command:str, OpenInputMethod, WriteInputMethod, AddParam:bool = False, Verbose=False, SimulationManager=None):
        SimInfo.update({'Command':Command,'Directory':Directory})
        UpdateInputFile(SimInfo, OpenInputMethod, WriteInputMethod, AddParam, Verbose)
        if SimulationManager is None:
            SimInfo.update({'SimulationManager':self.__class__})
        else:
            SimInfo.update({'SimulationManager':SimulationManager})

        Simulation(**SimInfo)

class Simulation():
    def __init__(self, **kwargs):
        self.Parameters = []
        self.Containers = []
        self.Values = []
        self.Directory = None
        self.Output = ''
        self.StartTime = None
        self.RunTime = 0
        self.Process = None
        self.Slurm = None
        self.PipeR = None
        self.PipeW = None
        self.Status = 'Idle'
        self.Verbose = False
        self.Options = []
        self.Command = []
        self.Data = {}
        self.Idx = None
        self.ConfigFilePath = None
        self.SimulationManager = self.__class__
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)

        self.Id = self.SimulationManager.NSimu
        self.SimulationManager.CurrentSimu.append(self)
        self.SimulationManager.NSimu = self.SimulationManager.NSimu + 1
        self.run_type = 'shell'
    @property
    def RunTime(self):
        if self.StartTime is not None:
            self.__RunTime = time.time()-self.StartTime
        else:
            self.__RunTime = 0
        return self.__RunTime

    @RunTime.setter
    def RunTime(self, RunTime):
        if self.StartTime is not None:
            self.__RunTime = time.time()-self.StartTime
        else:
            self.__RunTime = 0

    @property
    def Verbose(self):
        return self.__class__.Verbose

    @Verbose.setter
    def Verbose(self, Verbose):
        self.__class__.Verbose = Verbose

    @property
    def Status(self):
        self.GetStatus()
        return self.__Status

    @Status.setter
    def Status(self, Status):
        self.__Status = Status

    @property
    def Output(self):
        self.__Output = self.__Output + self.FlushOutput()
        return self.__Output

    @Output.setter
    def Output(self, Output):
        self.__Output = Output

    def get(self,Attr):
        if hasattr(self,Attr):
            return getattr(self,Attr)
        else:
            return None

    def rget(self,Attr):
        return rget(self,Attr)

    def LastLine(self):
        try:
            return self.Output.split('\n')[-2]
        except:
            return self.Output.split('\n')[-1]

    def GetStatus(self):
        if self.run_type == 'shell':
            if self.Process is not None :
                try:
                    s = self.Process.poll()
                    if s is None:
                        self.Status = 'R'
                    else:
                        self.Status = s
                except:
                    self.Status = 'U'
        elif self.run_type == 'slurm':
            if self.Slurm is not None:
                 try:
                     self.Status = pyslurm.job().get()[self.Slurm['job_id']]['job_state']
                 except:
                     self.Status = 'U'
    def Stop(self):
        self.Output
        try:
            self.Process.terminate()
            self.Status
            if self.run_type == 'shell':
                os.close(self.PipeW)
                os.close(self.PipeR)
        except:
            pass
        

    def Start(self, Command=None, **kwargs):
        self.run_type = 'shell'
        if Command is not None:
            self.Command = Command
        print('Starting simulation #{} ....)'.format(self.Id))

        #assert self.Command != [], 'Cannot start simulation with empty command'

        (self.PipeR, self.PipeW) = os.pipe()
        self.Process = subprocess.Popen(self.Command,
                                        stdout=self.PipeW,
                                        stderr=self.PipeW,
                                        stdin=subprocess.PIPE,
                                        cwd=self.Directory,
                                        **kwargs
                                        )
        self.StartTime = time.time()
        
    def StartSlurm(self, Command=None,**kwargs):
        self.run_type = 'slurm'
        if Command is not None:
            self.Command = Command
            
        if kwargs.get('log_dir') is None: 
            kwargs['log_dir'] = os.path.join(self.Directory,'logs')
            
        print('Starting simulation #{} ....)'.format(self.Id))

        assert self.Command != [] , 'Cannot start simulation with empty command'

        #(self.PipeR, self.PipeW) = os.pipe()
        self.Slurm = sbatch(self.Command,
                            chdir=self.Directory,
                            e='%j.log',
                            o='%j.log',
                            **kwargs
                            )
        try:
            self.PipeR = pyslurm.job().get()[self.Slurm.job_id]['std_out']
        except:
            self.PipeR = self.Slurm.slurm_kwargs['o']
        self.StartTime = time.time()

    def FlushOutput(self):
        if self.run_type == 'shell':
            try:
                buf = ''
                if self.PipeR is not None:
                    while len(select.select([self.PipeR], [], [], 0)[0]) == 1:
                        buf = buf+os.read(self.PipeR, 10024).decode()
            except:
                buf = 'Cannot read stdout'
            return buf
        elif self.run_type == 'slurm':
            try:
                with open( self.PipeR, "r") as f:
                    buf = f.readlines()[-1]
            except:
                buf = 'failed to read log file {}'.format(self.PipeR)
            return buf
        else:
            raise ValueError()
            
            


def UpdateInputFile(Dic:dict, LoadMethod, DumpMethod, AddParam=False, Verbose=False):
    Dic.update({'Parameters' : [p['ParamName'] for p in Dic['ParameterInfo'].values()],
         'Containers' : [p['Containers'] for p in Dic['ParameterInfo'].values()],
         'Values' :  [p['Value'] for p in Dic['ParameterInfo'].values()],
        'ConfigFilePath' : [ os.path.join(Dic['Directory'],CF) for CF in [p['ConfigFile'] for p in Dic['ParameterInfo'].values()]],
        })

    for P, C, V, FP in zip(*[Dic[k] for k in ['Parameters','Containers', 'Values','ConfigFilePath']]):
        UpdateInputValue(FP, P, C, V, LoadMethod, DumpMethod, AddParam, Verbose)

def UpdateInputValue(FilePath:str,ParamName:str, Containers:list, Value, LoadMethod, DumpMethod, AddParam=False, Verbose=False):
    if Verbose:
        print("Modifying parameter {} in containers {} with values {} in file: {}".format(ParamName,Containers,Value,FilePath))

    Config = LoadMethod(FilePath)
    if rget(Config, Containers+[ParamName]) is None and not AddParam:
        print(' Parameter {} in containers {} does not exisit. Skipping ...'.format(ParamName,Containers))
    else:
        rset(Config, Containers+[ParamName], Value)

    DumpMethod(FilePath,Config)

def rget(Dict:dict, KeyList:str or list ):
    """Iterate nested dictionary"""
    if type(KeyList) == str:
        KeyList = [KeyList]
    assert(KeyList != [])
    try:
        return reduce(dict.get, KeyList, Dict)
    except Exception as e:
        print(repr(e))

        return None

def rset(Dict:dict, KeyList:str or list, Value):
    """Iterate nested dictionary"""
    if type(KeyList) == str:
        KeyList = [KeyList]
    assert(KeyList != [])
    if len(KeyList)==1:
        Dict.__setitem__(KeyList[0], Value)
    else:
        Dic= rget(Dict,KeyList[:-1])
        if Dic is not None:
            Dic.__setitem__(KeyList[-1], Value)


def get_from_dict(dataDict, mapList):
    """Iterate nested dictionary"""
    return reduce(dict.get, mapList, dataDict)


def enumerated_product(*args):
        yield from zip(itertools.product(*(range(len(x)) for x in args)), itertools.product(*args))


def MakeSimFolder(Suffix, ReferenceDirectory, SimRootPath=None, OverWrite=False, Verbose=False):
    if SimRootPath is None:
        SimRootPath = ReferenceDirectory
    CopyFolder(ReferenceDirectory,os.path.expanduser(SimRootPath)+'_'+Suffix, OverWrite, Verbose)
    return os.path.expanduser(SimRootPath)+'_'+Suffix

def CopyFolder(from_path, to_path, OverWrite=False, Verbose=False):
    to_path = os.path.expanduser(to_path)
    from_path = os.path.expanduser(from_path)
    if Verbose: print('Copying {} to {}'.format(from_path,to_path))
    if os.path.exists(to_path):
        if OverWrite:
            shutil.rmtree(to_path, ignore_errors=True)
        else:
            print('Folder {} already exists. No copy made.'.format(to_path))
            return False
    try:
        try:
            shutil.copytree(from_path, to_path, dirs_exist_ok=True)
        except:
            shutil.copytree(from_path, to_path)  
        if Verbose: print('Successful copy {} to {} '.format(from_path,to_path))
        return True
    except Exception as E:
        print(E)
        return False

def MakeParameterArray(ParameterScan:dict, Format='value', Verbose=False):

        ListParams = [[(k,V,v['ConfigFile']) for V in v['Values']] for k,v in ParameterScan.items()]
        DimParams = tuple(len(v['Values']) for v in ParameterScan.values())
        if Verbose:
            print('ListParams:{}'.format(ListParams))
            print('Dimension Parameters:', DimParams)

        ArrSim = np.empty(shape=DimParams,dtype='object')
        for idx, p in enumerated_product(*ListParams):
            ListSuffix = []
            ParamInfo = {}

            for i,k in enumerate(p):
                ConfigFile = k[2]
                ParamName = k[0].split('.')[-1]
                Kws = k[0].split('.')[:-1]
                Value = k[1]

                ParamInfo[k] = {'ParamName':ParamName,'Containers': Kws,'Value':Value,'ConfigFile':ConfigFile}
                if Format == 'value':
                    ListSuffix.append('{}_{}'.format(ParamName,Value))
                elif Format == 'index':
                    ListSuffix.append('{}_{}'.format(ParamName,i))
                else:
                    raise ValueError('Format must be "value" or "index"')
            SimInfo = {'ParameterInfo':ParamInfo,'Idx': idx,'Suffix':'_'.join(ListSuffix)}
            ArrSim[idx] = SimInfo

        return ArrSim

def MakeParamInfo(Dic):
    ParamInfo = {}
    for k,v in Dic.items():
     ParamInfo[k] = {'ParamName' : k.split('.')[-1],
     'Containers' : k.split('.')[:-1],
     'Value': v['Value'] ,
     'ConfigFile': v['ConfigFile']}
    return ParamInfo