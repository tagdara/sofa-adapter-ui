#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

import devices
import json
import asyncio

import concurrent.futures
import aiofiles
import datetime
import base64
import ssl
import uuid
import subprocess
import copy
import select


class EndpointHealth(devices.EndpointHealth):
    
    @property            
    def connectivity(self):
        if self.nativeObject and 'service' in self.nativeObject and self.nativeObject['service'] and 'ActiveState' in self.nativeObject['service']:
            if self.nativeObject['service']['ActiveState']=='active':
                return 'OK'
        return 'UNREACHABLE'

class VirtualEndpointHealth(devices.EndpointHealth):
    
    @property            
    def connectivity(self):
        
        for cap in self.nativeObject['capabilities']:
            if cap['endpointId'] not in self.adapter.dataset.devices:
                return 'UNREACHABLE'
        return 'OK'

class VirtualController(devices.capabilityInterface):
    
    def __init__(self, device=None, controller_type=None, real_device=None):
        super().__init__(device=device)    
        self._controller_type=controller_type
        self.real_endpointId=real_device
        self.real_adapter=real_device.split(':',1)[0] 
        #self.log.info('real device: %s' % self.adapter.dataset.devices[real_device])
        self._props=[]
        
        if real_device not in self.adapter.dataset.devices:
            fake_controller=getattr(devices,self.controller)()
            self._props=fake_controller.props

        else:
            for cap in self.adapter.dataset.devices[real_device]['capabilities']:
                if cap['interface']!='Alexa' and cap['interface'].split('.')[1]==controller_type:
                    self._namespace=cap['interface'].split('.')[0]
                    if 'properties' in cap:
                        for supp in cap['properties']['supported']:
                            self._props.append(supp['name'])
        
    async def get_real_device_state(self):
        try:
            #reportState=self.adapter.dataset.ReportState(self.real_endpointId)
            if hasattr(self.adapter, 'server') and self.real_endpointId in self.adapter.dataset.devices:
                reportState=self.adapter.dataset.ReportState(self.real_endpointId)
                if self.real_adapter not in self.adapter.adapters:
                    self.log.warning('.! warning - no device state for %s from real device %s (adapter %s not activated)' % (self.device.endpointId, self.real_endpointId, self.real_adapter))
                    return False
                adapter_response=await self.adapter.server.adapter_post(self.adapter.adapters[self.real_adapter]['url'], reportState, token=self.adapter.adapters[self.real_adapter]['response_token'])

                #self.log.info('.. get real device state: %s %s %s' % (self.nativeObject, self.real_endpointId, adapter_response))
                updates={}
                
                if 'context' not in adapter_response:
                    self.log.error('!! invalid response for real device state %s - %s' % (self.real_endpointId, adapter_response))
                else:
                    for item in adapter_response['context']['properties']:
                        if item['namespace']!='Alexa' and item['name'] in self._props and item['namespace'].split('.')[1]==self._controller_type:
                            if 'cached' not in self.nativeObject or item['name'] not in self.nativeObject['cached'] or self.nativeObject['cached'][item['name']]!=item['value']:
                                updates[item['name']]=item['value']
                    if updates:
                        #self.log.info('.. real device state updates for virtual %s: %s' % (self.device.endpointId, updates))
                        await self.adapter.dataset.ingest( { 'virtual' : { self.deviceid: { "cached":  updates }}})
 
        except:
            self.log.error('!! Error getting controller update %s %s' % (self.real_endpointId, self.adapter.adapters.keys()), exc_info=True)                    

    async def get_real_device_change(self, report):
        try:
            updates={}

            if 'context' in report:
                for item in report['context']['properties']:
                    if item['namespace']!='Alexa' and item['name'] in self._props and item['namespace'].split('.')[1]==self._controller_type:
                        if 'cached' not in self.nativeObject or item['name'] not in self.nativeObject['cached'] or self.nativeObject['cached'][item['name']]!=item['value']:
                            updates[item['name']]=item['value']
                            
            if 'event' in report and'payload' in report['event']:
                for item in report['event']['payload']['change']['properties']:
                    if item['namespace']!='Alexa' and item['name'] in self._props and item['namespace'].split('.')[1]==self._controller_type:
                        if 'cached' not in self.nativeObject or item['name'] not in self.nativeObject['cached'] or self.nativeObject['cached'][item['name']]!=item['value']:
                            updates[item['name']]=item['value']

            if updates:
                self.log.info('.. real device state updates for virtual %s: %s' % (self.device.endpointId, updates))
                await self.adapter.dataset.ingest( { 'virtual' : { self.deviceid: { "cached":  updates }}})
 
        except:
            self.log.error('!! Error getting controller update %s: %s' % (self.real_endpointId, report), exc_info=True)                    



    def __getattr__(self, attr):

        #self.log.info('GETTER:%s %s' % (self.device.endpointId, attr))
        try:
            if 'cached' in self.nativeObject and attr in self.nativeObject['cached']:
                #self.log.info('GETTER: %s' % self.nativeObject['cached'][attr])
                return self.nativeObject['cached'][attr]
        except KeyError: 
            # if cached doesn't already exist on the nativeObject
            pass
        except:
            self.log.error('!! error getting virtual attribute', exc_info=True)
        return None
            

    @property
    def controller(self):
        return self._controller_type

    @property
    def props(self):
        return self._props


class AdapterHealth(devices.AdapterHealth):
    
    @property
    def datasize(self):
        try:
            if 'state' in self.nativeObject and 'native_size' in self.nativeObject['state']:
                return self.nativeObject['state']['native_size']
            else:
                return 0
        except:
            self.log.error('!! Error getting datasize', exc_info=True)

    @property
    def url(self):
        try:
            if 'rest' in self.nativeObject and 'url' in self.nativeObject['rest']:
                return self.nativeObject['rest']['url']
            else:
                return ''
        except:
            self.log.error('!! Error getting url', exc_info=True)


    @property
    def logged(self):
        try:
            if 'state' in self.nativeObject and 'logged' in self.nativeObject['state']:
                return self.nativeObject['state']['logged']
        except:
            self.log.error('!! Error getting url', exc_info=True)
        return {'ERROR':0, 'INFO':0}
        

    @property
    def startup(self):
        try:
            if 'service' in self.nativeObject and self.nativeObject['service'] and 'ActiveState' in self.nativeObject['service']:
                if self.nativeObject['service']['ActiveState']=='active':
                    return datetime.datetime.fromtimestamp(self.nativeObject['service']['ExecMainStartTimestamp']).isoformat()
            if 'rest' in self.nativeObject and 'startup' in self.nativeObject['rest']:
                return self.nativeObject['rest']['startup']
        except:
            self.log.error('!! Error getting startup time', exc_info=True)
            
        return ''

class ServiceController(devices.ServiceController):

    @property
    def activeState(self):
        try:
            if 'service' in self.nativeObject and 'ActiveState' in self.nativeObject['service']:
                return self.nativeObject['service']['ActiveState']
        except:
            self.log.error('!! Error getting ActiveState', exc_info=True)
        return "unknown"
        
        
    @property
    def subState(self):
        try:
            if self.nativeObject and 'service' in self.nativeObject and 'SubState' in self.nativeObject['service']:
                return self.nativeObject['service']['SubState']
        except:
            self.log.error('!! Error getting SubState', exc_info=True)
        return "unknown"
        
        
    @property
    def pid(self):
        try:
            if 'service' in self.nativeObject and 'ExecMainPID' in self.nativeObject['service']:
                return self.nativeObject['service']['ExecMainPID']
        except:
            self.log.error('!! Error getting pid', exc_info=True)
        return 0
                
                
    @property
    def process(self):
        try:
            if 'service' in self.nativeObject and self.nativeObject['service'] and 'Process' in self.nativeObject['service']:
                return self.nativeObject['service']['Process']
        except:
            self.log.error('!! Error getting Process', exc_info=True)
        return ""


    @property
    def loadState(self):
        try:
            if self.nativeObject and 'service' in self.nativeObject and 'LoadState' in self.nativeObject['service']:
                return self.nativeObject['service']['LoadState']
        except:
            self.log.error('!! Error getting LoadState', exc_info=True)
        return ""

    @property
    def startTime(self):
        try:
            if 'service' in self.nativeObject and 'ExecMainStartTimestamp' in self.nativeObject['service']:
                return datetime.datetime.fromtimestamp(self.nativeObject['service']['ExecMainStartTimestamp']).isoformat()
        except:
            self.log.error('!! Error getting ExecMainStartTimestamp', exc_info=True)
        return ""


class PowerController(devices.PowerController):

    @property            
    def powerState(self):
        if 'service' in self.nativeObject and 'ActiveState' in self.nativeObject['service']:
            if self.nativeObject['service']['ActiveState']=='active':
                return 'ON'
        return 'OFF'
        

    async def TurnOn(self, correlationToken='', **kwargs):
        try:
            self.log.info('.. starting or restarting adapter service: %s' % self.nativeObject['name'])
            restart_state={ "state": { "logged": {"ERROR":0, "INFO":0}}, "service": await self.adapter.get_service_status(self.nativeObject['name']) }
            await self.adapter.dataset.ingest({'adapters': { self.nativeObject['name'] : restart_state }})
            await self.adapter.get_service_status(self.nativeObject['name'], action="restart")
            #stdoutdata = subprocess.getoutput("/opt/sofa-server/svc %s" % self.nativeObject['name'])
            await self.adapter.dataset.ingest({'adapters': { self.nativeObject['name'] : { "service": await self.adapter.get_service_status(self.nativeObject['name'])}}})
            #return web.Response(text=stdoutdata)
            return self.device.Response(correlationToken)
        except:
            self.log.error('!! Error restarting adapter', exc_info=True)
            return self.device.Response(correlationToken)

    async def TurnOff(self, correlationToken='', **kwargs):
        try:
            pids_output=subprocess.check_output(["pgrep","-f","sofa-server/adapters/%s/" % self.nativeObject['name']])
            pids=pids_output.decode().strip().split('\n')
            stdoutdata=""
            for pid in pids:
                stdoutdata += subprocess.getoutput("kill -9 %s" % pid)
            self.log.info('.. results from kill %s (%s): %s' % (self.nativeObject['name'], pids, stdoutdata))
            #return web.Response(text=stdoutdata)
            return self.device.Response(correlationToken)
        except:
            self.log.error('!! Error stopping adapter', exc_info=True)
            return self.device.Response(correlationToken)  
    

