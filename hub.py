#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase, adapterbase, configbase
from sofacollector import SofaCollector
from event_gateway import Event_Gateway
from controllers import PowerController, ServiceController, AdapterHealth, VirtualController, VirtualEndpointHealth, EndpointHealth

import devices
import json
import asyncio
import aiohttp

import concurrent.futures
import aiofiles
import datetime
import base64
import ssl
import uuid
import subprocess
import pystemd
import copy
import select

from pystemd.systemd1 import Unit
from pystemd.dbuslib import DBus
from wakeonlan import send_magic_packet

class hub(sofabase):
    
    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.web_address=self.set_or_default("web_address", mandatory=True)
            self.web_port=self.set_or_default("web_port", default=6443)
            self.token_secret=self.set_or_default("token_secret", mandatory=True)
            self.certificate=self.set_or_default("certificate", mandatory=True)
            self.certificate_key=self.set_or_default("certificate_key", mandatory=True)
            self.token_expires=self.set_or_default("token_expires", default=604800)
            self.error_threshold=self.set_or_default("error_threshold", default=5)

    class adapterProcess(SofaCollector.collectorAdapter):

        def __init__(self, log=None, loop=None, dataset=None, config=None, **kwargs):
            super().__init__(log=log, loop=loop, dataset=dataset, config=config)
            #self.server=None
            self.dataset.nativeDevices['adapters']={}
            self.dataset.nativeDevices['virtual']={}
            self.poll_time=600
            self.unavailable_devices={}
            self.cached_wol_endpoints=[] 
            self.error_threshold=5
            self.recent_changes=[]
            self.recent_limit=25
            self.adapters={}
            
        async def pre_activate(self):
            
            # This adapter does not activate but this defines the point in the timeline for the correct start
            self.log.info('.. loading predefined adapter list')
            self.cached_wol_endpoints=self.loadJSON("wol_endpoints")
            await self.add_defined_adapters()
            await self.add_virtual_devices()
            asyncio.create_task(self.start_service_monitor())
            
        async def start(self):
            self.log.info('.. Starting sofa hub server')
            self.api_consumers=self.loadJSON("api_consumers")
            self.server = Event_Gateway(config=self.config, api_consumers=self.api_consumers, loop=self.loop, log=self.log, dataset=self.dataset, adapter=self, adapters=self.adapters)
            await self.server.initialize()

        async def add_defined_adapters(self):
            
            try:
                predefined=self.loadJSON('api_consumers')
                for adapter in predefined:
                    try:
                        newadapter={"name":adapter, "api_key": predefined[adapter]['api_key'], "state":{}, "service":{}, "rest": {}}
                        if 'url' in predefined[adapter]:
                            newadapter['url']=predefined[adapter]['url']
                        else:
                            newadapter['url']=""
                        newadapter['service']=await self.get_service_status(adapter)
                        await self.dataset.ingest({'adapters': { adapter : newadapter}})
                    except:
                        self.log.error('!! error initializing adapter %s' % adapter, exc_info=True)
                        
                asyncio.create_task(self.poll_loop())
                
            except:
                self.log.error('!! Error populating adapters', exc_info=True)

        async def add_virtual_devices(self):
            
            try:
                virtual=self.loadJSON('virtual')
                for device in virtual:
                    try:
                        virtual[device]['ready']=True
                        await self.dataset.ingest({'virtual': { device:  virtual[device] }})
                    except:
                        self.log.error('!! error initializing adapter %s' % adapter, exc_info=True)
                        
                asyncio.create_task(self.poll_loop())
                
            except:
                self.log.error('!! Error populating adapters', exc_info=True)
                

        async def virtual_device_refresh(self, adaptername):
            
            for device in self.dataset.localDevices:
                if device in self.dataset.nativeDevices['virtual']:
                    for cap in self.dataset.nativeDevices['virtual'][device]['capabilities']:
                        if self.server.device_adapter_map[cap['endpointId']]==adaptername:
                            self.log.info('locals: %s' % self.dataset.localDevices)
                            self.log.info('.. updating virtual device %s due to change with %s on %s' % (self.dataset.localDevices[device], cap['endpointId'], adaptername))
                            await getattr(self.dataset.localDevices[device], cap['controller']).get_real_device_state()   



        async def poll_loop(self):
            while self.running:
                try:
                    # This will check on the adapter services locally running on the hub device
                    # it should only be run periodically since it generates a lot of CPU usage
                    # the ongoing event monitor should catch most changes after the initial load
                    
                    await self.adapter_checker()
                    await asyncio.sleep(self.poll_time)
                except:
                    self.log.error('!! Error polling for data', exc_info=True)

        async def adapter_checker(self):
            
            try:
                workingadapters=self.dataset.nativeDevices['adapters']
                for adapter in workingadapters:
                    try:
                        adapterstate={"state":{}, "service":{}, "rest": {}}
                        if adapter in self.dataset.nativeDevices['adapters']:
                            adapterstate['service']=await self.get_service_status(adapter)
                            await self.dataset.ingest({'adapters': { adapter : adapterstate}})
                    except:
                        self.log.info('!! error during adapter check %s' % (adapter), exc_info=True)
                    
            except:
                self.log.error('!! Error listing adapters', exc_info=True)

        async def check_adapter_health(self, adaptername):
            try:
                reqstart=datetime.datetime.now()
                if adaptername in self.adapters:
                    url = '%s/status' % (self.adapters[adaptername]['url'] )
                    async with aiohttp.ClientSession() as client:
                        async with client.get(url) as response:
                            result=await response.read()
                            result=result.decode()
                            result=json.loads(result)
                            if result['logged']['ERROR']<self.error_threshold:
                                # Not enough errors to force a restart
                                return True
                                asyncio.create_task(self.restart_adapter(adaptername))
            except aiohttp.client_exceptions.ClientOSError:
                self.log.error('.. status request failed after %s seconds for %s (Client Error/Connection Reset)' % ((datetime.datetime.now()-reqstart).total_seconds(), adaptername))
            except concurrent.futures._base.CancelledError:
                self.log.error('.. status request cancelled after %s seconds for %s' % ((datetime.datetime.now()-reqstart).total_seconds(), adaptername))
            except:
                self.log.error('!! Error after %s seconds getting adapter status for %s' % ((datetime.datetime.now()-reqstart).total_seconds(), adaptername), exc_info=True)
            
            # if there was any error message or too many adapter errors, try to restart local adapters
            try:
                if adaptername in self.adapters:
                    if self.config.rest_address in self.adapters[adaptername]['url']:
                        asyncio.create_task(self.get_service_status(adaptername, 'restart'))
                    else:
                        self.log.info('!! warning - cannot restart remote adapter: %s %s' % (adaptername, self.adapters[adaptername]['url']))
            except:
                self.log.error('!! Error trying to restart %s' % adaptername, exc_info=True)

            return False


        async def get_service_status(self,adaptername, action=None):
            try:
                return await self.loop.run_in_executor(None, self.get_service_status_sync, adaptername, action)
            except:
                self.log.error('!! Error in async wrapper for getting adapter service status', exc_info=True)


        def get_service_status_sync(self, adaptername, action=None):
            
            # Warning - this process is not Async and needs to be wrapped in an executor to complete without blocking
            # and causing delays in command passing
            
            try:
                unit = Unit(str.encode('sofa-%s.service' % adaptername))
                unit.load()
                #unit.Unit.ActiveState
                #unit.Unit.SubState
                result=None
                if action=="stop":
                    result=unit.Unit.Stop(b'replace')
                elif action=="start":
                    result=unit.Unit.Start(b'replace')
                elif action=="restart":
                    result=unit.Unit.Restart(b'replace')
                
                if result:
                    self.log.info('.. result for %s service %s : %s' % (adaptername, action, result))
                
                processes=unit.Service.GetProcesses()
                mainprocess=""
                for process in processes:
                    if process[1]==unit.Service.MainPID:
                        mainprocess=process[2].decode()
                #self.log.info('~~~~ %s - %s' % (adaptername, unit.Service.properties))
                #for prop in unit.Unit.properties:
                #    self.log.info('PROP: %s = %s' % (prop, getattr(unit.Unit, prop)))
                return {
                    "id": unit.Unit.Id.decode(),
                    "ActiveState": unit.Unit.ActiveState.decode(), 
                    "SubState": unit.Unit.SubState.decode(), 
                    "ExecMainPID": unit.Service.MainPID, 
                    "Process": mainprocess,
                    "ExecMainStartTimestamp": int(str(unit.Service.ExecMainStartTimestamp)[:10]), 
                    "LoadState": unit.Unit.LoadState.decode()
                }
            except:
                self.log.error('!! Error getting adapter service status', exc_info=True)
                return {}


        async def start_service_monitor(self):
            try:
                return await self.loop.run_in_executor(None, self.service_monitor)
            except:
                self.log.error('!! Error in async wrapper for service monitor', exc_info=True)


        def service_monitor(self):
            
            def process_service_change(msg, error=None, userdata=None, metadata=None):
                try:
                    # read the message True means read Header... usually not needed, we just
                    # add it here because this is an example
                    msg.process_reply(True)

                    interface=msg.headers['Interface'].decode().split('/')[-1].replace("_2d","-").replace("_2e",".")
                    
                    service_adapter=None
                    for adapter in self.dataset.nativeDevices['adapters']:
                        if self.dataset.nativeDevices['adapters'][adapter]['service']['id']==interface:
                            service_adapter=adapter
                            
                    if not service_adapter:
                        #self.log.info('.. did not find adapter for service info: %s' % interface)
                        return {}
                    
                    props=msg.body[1]
                    props={}
                    for item in msg.body[1]:
                        try:
                            props[item.decode()]=msg.body[1][item].decode()
                        except:
                            props[item.decode()]=msg.body[1][item]
                    service_data={}
                    if 'ActiveState' in props:
                        service_data['ActiveState']=props['ActiveState']
                        service_data['SubState']=props['SubState']

                    if 'ExecMainPID' in props:
                        service_data['ExecMainPID']=props['ExecMainPID']
                        service_data['ExecMainStartTimestamp']=props['ExecMainStartTimestamp']

                    if service_data:
                        #self.log.info('~~ Service Data: %s %s' % (service_adapter, service_data))
                        self.loop.call_soon_threadsafe(self.dataset.ingest, {'adapters': { service_adapter : service_data}})
                except:
                    self.log.error('!! Error processing service change', exc_info=True)
                return service_data
        
            try:
                with DBus() as bus:
                    bus.match_signal( 
                        b"org.freedesktop.systemd1", 
                        None, 
                        b"org.freedesktop.DBus.Properties",
                        b"PropertiesChanged", 
                        process_service_change, 
                        None,
                    )
                    fd = bus.get_fd()
                    self.log.info('.. starting service monitor')
                    while self.running:
                        select.select([fd], [], [])  # wait for message
                        bus.process()  # execute all methods (driver)
            except:
                self.log.error('!! Error in service monitor', exc_info=True)


        async def wake_on_lan(self, endpointId):
            
            try:
                if endpointId in self.cached_wol_endpoints:
                    macs=self.cached_wol_endpoints[endpointId]
                
                elif self.dataset.deviceHasCapability(endpointId, 'WakeOnLANController'):
                    for devcap in self.dataset.devices[endpointId]['capabilities']:
                        if devcap['interface']=='Alexa.WakeOnLANController':
                            macs=devcap['configuration']['MACAddresses']
                if macs:
                    for mac in macs:
                        self.log.info('.. sending wake on lan to %s' % mac)
                        #mac.replace(':','.')
                        send_magic_packet(mac.replace(':','.'))

            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)


        async def handleStateReport(self, message, source=None):
            try:
                await super().handleStateReport(message, source=source)
                await self.server.add_collector_update(message, source=source)
            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)


        async def handleAddOrUpdateReport(self, message, source=None):
            try:
                await super().handleAddOrUpdateReport(message, source=source)
                await self.server.add_collector_update(message, source=source)
            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)


        async def oldhubhandleAddOrUpdateReport(self, message, source=None):
            try:


                # 10/2/20Migrating to use the built-in function from the collector base

                
                # Hub should override the base collector framework to prevent multiple requests of the 
                # state information.  Should this be done here or in the individual collectors?  Some collectors
                # will only need a subset
                
                #await super().handleAddOrUpdateReport(message)
                devlist=message['event']['payload']['endpoints']
                if devlist:
                    for dev in devlist:
                        self.dataset.devices[dev['endpointId']]=dev
                    
                if message and self.server:
                    try:
                        if self.config.log_changes:
                            self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        await self.server.add_collector_update(message, source=source)

                    except:
                        self.log.warn('!. bad or empty AddOrUpdateReport message not sent to SSE: %s' % message, exc_info=True)
            except:
                self.log.error('Error updating from change report', exc_info=True)


        async def handleChangeReport(self, message, source=None):
            try:

                #await super().handleChangeReport(message)
                if message:
                    if self.caching_enabled:
                        try:
                            endpointId=message['event']['endpoint']['endpointId']
                            self.state_cache[endpointId]=list(message['context']['properties'])
                            for prop in message['event']['payload']['change']['properties']:
                                self.state_cache[endpointId].append(prop)
                            #self.log.info('~~ Change report cached for %s: %s' % (endpointId, self.state_cache[endpointId]))
                        except:
                            self.log.error("!! Error caching state from change report", exc_info=True)


                    try:
                        if self.config.log_changes:
                            self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        await self.server.add_collector_update(message, source=source)
                        await self.cache_virtual_devices(message)
                        await self.add_to_recent(message)

                    except:
                        self.log.warn('!. bad or empty ChangeReport message not sent to SSE: %s' % message, exc_info=True)
            except:
                self.log.error('Error updating from change report', exc_info=True)


        async def handleDeleteReport(self, message, source=None):
            try:
                self.log.info('!! Handle Delete Report: %s' % message)
                #await super().handleDeleteReport(message)
                if message:
                    try:
                        #if self.config.log_changes:
                        self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        await self.server.add_collector_update(message, source=source)

                    except:
                        self.log.warn('!. bad or empty DeleteReport message not sent to SSE: %s' % message, exc_info=True)

            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)


        # Adapter Overlays that will be called from dataset
        async def addSmartDevice(self, path):
            
            #self.log.info('path %s %s' % ( path,self.dataset.localDevices.keys()) )
            try:
                if path.split("/")[1]=="adapters":
                    nativeObject=self.dataset.getObjectFromPath(self.dataset.getObjectPath(path))
                    #self.log.info('native: %s %s' % (path,nativeObject))
                    deviceid=path.split("/")[2]
                    endpointId="hub:adapters:%s" % deviceid
                    if endpointId not in self.dataset.localDevices: 
                        device=devices.alexaDevice('hub/adapters/%s' % deviceid, deviceid, displayCategories=['ADAPTER'], adapter=self)
                        device.PowerController=PowerController(device=device)
                        device.AdapterHealth=AdapterHealth(device=device)
                        device.EndpointHealth=EndpointHealth(device=device)
                        device.ServiceController=ServiceController(device=device)
                        return self.dataset.add_device(device) 

                if path.split("/")[1]=="virtual":
                    nativeObject=self.dataset.getObjectFromPath(self.dataset.getObjectPath(path))
                    deviceid=path.split("/")[2]
                    endpointId="hub:adapters:%s" % deviceid
                    if endpointId not in self.dataset.localDevices: 
                        device=devices.alexaDevice('hub/virtual/%s' % deviceid, nativeObject['friendlyName'], displayCategories=nativeObject['displayCategories'], adapter=self)
                        device.EndpointHealth=VirtualEndpointHealth(device=device)
                        for cap in nativeObject['capabilities']:
                            setattr(device, cap['controller'], VirtualController(device=device, controller_type=cap['controller'], real_device=cap['endpointId']))
                            await getattr(device, cap['controller']).get_real_device_state()
                        return self.dataset.add_device(device) 

            except:
                self.log.error('!! Error defining smart device', exc_info=True)
                return False
                

        async def virtualAddDevice(self, endpointId, device):
            try:
                wol_controller=self.dataset.device_capability(endpointId, 'WakeOnLANController')
                if wol_controller:
                    self.log.info('.. WOL device: %s %s' % (endpointId, wol_controller['configuration']['MACAddresses']))
                    self.cached_wol_endpoints[endpointId]=wol_controller['configuration']['MACAddresses']
                    self.saveJSON('wol_endpoints', self.cached_wol_endpoints)
                
                for dev in self.dataset.nativeDevices['virtual']:
                    for cap in self.dataset.nativeDevices['virtual'][dev]['capabilities']:
                        if endpointId==cap['endpointId']:
                            virtual_endpointId='hub:virtual:%s' % dev
                            controller=getattr(self.dataset.localDevices[virtual_endpointId], cap['controller'])
                            await controller.get_real_device_state()

            except:
                self.log.info('!! Error checking backing devices for virtual: %s %s' % (endpointId, device), exc_info=True)


        async def cache_virtual_devices(self, message):
            try:
                # With primary caching added back to Sofa, this should be refactored to just read from the cache
                # instead of maintaining a second copy
                for dev in self.dataset.nativeDevices['virtual']:
                    for cap in self.dataset.nativeDevices['virtual'][dev]['capabilities']:
                        if message['event']['endpoint']['endpointId']==cap['endpointId']:
                            virtual_endpointId='hub:virtual:%s' % dev
                            controller=getattr(self.dataset.localDevices[virtual_endpointId], cap['controller'])
                            await controller.get_real_device_change(message)

            except:
                self.log.info('!! Error checking backing devices for virtual', exc_info=True)
        
        async def virtualChangeHandler(self, endpointId, prop):
            self.log.info('!! VCHANGE: %s %s' % endpointId, prop)


        async def virtual_notify(self, message):
            try:
                # This is a shim that takes changeReports from Hub for virtual devices or adapters and then
                # sends them out to all of the SSE collectors
                
                # DeleteReports currently break this by not having endpoint
                if 'endpoint' in message['event']:
                    try:
                        source=self.server.device_adapter_map[message['event']['endpoint']['endpointId']]
                    except:
                        source=None

                    await self.server.process_gateway_message(message, source=source)
            except:
                self.log.error('!! Error in virtual notify for %s' % message, exc_info=True)


        async def virtualList(self, itempath, query={}):
            try:
                if itempath=="activations":
                    return self.server.pending_activations
            except:
                self.log.error('Error getting virtual list for %s' % itempath, exc_info=True)
            return {}


        async def add_to_recent(self, message):
            try:
                self.recent_changes.append(message)
                self.recent_changes=self.recent_changes[-1*self.recent_limit:]
            except:
                self.log.error('Error getting virtual list for %s' % itempath, exc_info=True)


        async def last_cache_update(self):
            
            latest_time=None
            self.log.info('.. getting latest update from %s items' % len(self.state_cache.keys()))
            try:
                for item in self.state_cache:
                    for prop in self.state_cache[item]['context']['properties']:
                        working=prop['timeOfSample'].split('.')[0].replace('Z','')
                        working=datetime.datetime.strptime(working, '%Y-%m-%dT%H:%M:%S')
                        if latest_time==None or working>latest_time:
                            latest_time=working
                self.log.info('.. latest update: %s' % working)
                return working
            except:
                self.log.error('!! error getting date for latest cache update', exc_info=True)

        async def cached_state_report(self, endpointId, correlationToken=None):
            try:
                response={ 
                    'event': {
                        'header': {
                            'name': 'StateReport', 
                            'payloadVersion': '3', 
                            'messageId': str(uuid.uuid1()), 
                            'namespace': 'Alexa'
                        },
                        'endpoint': {
                            'endpointId': endpointId,
                            'scope': {'type': 'BearerToken', 'token': ''}, 
                            'cookie': {}
                        }
                    }
                }
                
                if correlationToken:
                    response['event']['header']['correlationToken']=correlationToken
                    
                response['context']={ "properties" : self.state_cache[endpointId] }
                return response
            except:
                self.log.error('!! error generating cached state report %s' % endpointId, exc_info=True)
            return {}
            
                
        async def request_state_reports(self, device_list, cache=True):
            try:
                results=[]
                req_start=datetime.datetime.now()
                web_requests=[]
                cache_results=[]
                for dev in device_list:  
                    if dev in self.state_cache:
                        #self.log.info('.. getting %s from cache' % dev)
                        cache_results.append(await self.cached_state_report(dev))
                    else:
                        web_requests.append(self.dataset.requestReportState(dev))
                if len(web_requests)>0:
                    self.log.info('.. state cache misses: %s' % len(web_requests))
                    results = await asyncio.gather(*web_requests)

                results=results+cache_results
                self.log.info('.. request_state_reports: %s seconds / %s items / %s cache misses /  %s results' % ((datetime.datetime.now()-req_start).total_seconds(), len(device_list), len(web_requests), len(results)))
            except:
                self.log.error('!! error generating cached state report %s' % endpointId, exc_info=True)
            return results


        async def virtualList(self, itempath, query={}):

            try:
                self.log.info('list request: %s %s' % (itempath, query))
                itempath=itempath.split('/')
                if itempath[0]=="recent":
                    return self.recent_changes
                if itempath[0]=="map":
                    return self.server.device_adapter_map

            except:
                self.log.error('!! error generating list %s %s' % (itempath, query), exc_info=True)
            return {}



if __name__ == '__main__':
    adapter=hub(name='hub')
    adapter.start()
