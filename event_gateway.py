#!/usr/bin/python3

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))
import devices

import datetime

import json
import ssl
import uuid
import asyncio
import aiohttp
from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response

import concurrent.futures
from auth import api_consumer, Auth



class Event_Gateway():
    
    def __init__(self, api_consumers=None, loop=None, log=None, dataset=None, adapter=None, config=None, adapters=None):
        self.config=config
        self.api_consumers=api_consumers
        self.log=log
        self.loop = loop
        self.adapter=adapter
        self.dataset=dataset
        self.imageCache={}
        self.adapterTimeout=2
        self.slow_post=2
        self.sse_updates=[]
        self.sse_last_update=datetime.datetime.now(datetime.timezone.utc)
        self.active_sessions={}
        self.pending_activations=[]
        self.device_adapter_map={}
        self.restTimeout=4
        self.conn = aiohttp.TCPConnector()
        self.adapters_not_responding=[]
        self.queued_for_state=[]
        self.adapters=adapters

        
    async def initialize(self):

        try:
            self.device_adapter_map=self.adapter.load_cache('adapter_map')
            self.auth=Auth(secret=self.config.token_secret, log=self.log, token_expires=self.config.token_expires)
            for consumer in self.api_consumers:
                api_consumer.objects.create(name=consumer, api_key=self.api_consumers[consumer]['api_key'])
            
            self.serverApp = aiohttp.web.Application(middlewares=[self.auth.middleware])
            
            self.serverApp.router.add_post('/', self.event_gateway_handler)
            self.serverApp.router.add_get('/status', self.status_handler)
            self.serverApp.router.add_post('/activate', self.activation_handler)
            self.serverApp.router.add_post('/refresh', self.activation_refresh_handler)
            self.serverApp.router.add_post('/activations/{cmd}', self.activation_approve_handler)
            self.serverApp.router.add_get('/activations', self.activations_handler)

            self.serverApp.router.add_get('/list/{list:.+}', self.listHandler)
            self.serverApp.router.add_post('/list/{list:.+}', self.listPostHandler)
            
            self.serverApp.router.add_post('/add/{add:.+}', self.adapterAddHandler)
            self.serverApp.router.add_post('/del/{del:.+}', self.adapterDelHandler)
            self.serverApp.router.add_post('/save/{save:.+}', self.adapterSaveHandler)
            
            self.serverApp.router.add_get('/image/{item:.+}', self.imageHandler)
            self.serverApp.router.add_get('/thumbnail/{item:.+}', self.imageHandler)

            #self.serverApp.router.add_get('/pending-activations', self.get_user)
            self.serverApp.router.add_get('/eventgateway/status', self.status_handler)   
            self.serverApp.router.add_post('/eventgateway/activate', self.activation_handler)
            self.serverApp.router.add_post('/eventgateway', self.event_gateway_handler)
            # Add CORS support for all routes so that the development version can run from a different port
            self.cors = aiohttp_cors.setup(self.serverApp, defaults={
                "*": aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_methods='*', allow_headers="*") })

            for route in self.serverApp.router.routes():
                self.cors.add(route)

            self.runner=aiohttp.web.AppRunner(self.serverApp)
            await self.runner.setup()

            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self.ssl_context.load_cert_chain(self.config.certificate, self.config.certificate_key)

            self.site = aiohttp.web.TCPSite(self.runner, self.config.web_address, self.config.web_port, ssl_context=self.ssl_context)
            await self.site.start()

        except:
            self.log.error('Error with ui server', exc_info=True)
            
    def login_required(func):
        def wrapper(self, request):
            if not request.api_consumer:
                raise web.HTTPUnauthorized()
            return func(self, request)
        return wrapper

    def json_response(self, body='', **kwargs):
        try:
            kwargs['body'] = json.dumps(body, default=self.date_handler).encode('utf-8')
            kwargs['content_type'] = 'application/json'
            return aiohttp.web.Response(**kwargs)
        except:
            self.log.error('!! error with json response', exc_info=True)
            return aiohttp.web.Response({'body':''})

    async def loadData(self, jsonfilename):
        try:
            with open(os.path.join(self.config.data_directory, '%s.json' % jsonfilename),'r') as jsonfile:
                return json.loads(jsonfile.read())
        except:
            self.log.error('Error loading pattern: %s' % jsonfilename,exc_info=True)
            return {}

            
    def date_handler(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            raise TypeError


    # handler to authorize communication with adapters
    # When adapters first start, they send an activation request to hub to validate their connectivity.

    async def activation_handler(self, request):
        try:
            body=await request.read()
            data=json.loads(body.decode())
            #self.log.info('.. activation request for %s' % data['name'])
            check=await self.auth.get_token_from_api_key(data['name'], data['api_key'])
            if check:
                expiration=datetime.datetime.now() + datetime.timedelta(seconds=(self.config.token_expires-5))
                if 'url' in data:
                    self.adapters[data['name']]=data
                    if data['name'] in self.adapters_not_responding:
                        self.adapters_not_responding.remove(data['name'])
                    asyncio.create_task(self.discover_activated_adapter(data['name'], data['url'], data['response_token']))
                    asyncio.create_task(self.update_adapter_state_and_url(data['name'], data))               
                self.log.info('+> %s - activated adapter until %s' % (data['name'], expiration.isoformat()))
                return self.json_response({'token': check, 'expiration': expiration.isoformat() })
            else:   
                already_pending=False
                for item in self.pending_activations:
                    if item['name']==data['name']:
                        already_pending=True
                        break
                    
                if not already_pending:
                    self.pending_activations.append(data)
                return self.json_response({'activation': 'pending'})
        except:
            self.log.error('!! error with activation request', exc_info=True)
        return self.json_response({'activation': 'failed'})


    async def update_adapter_state_and_url(self, adapter, adapterdata):
        
        try:
            if adapter in self.dataset.nativeDevices['adapters']:
                adapterstate={ "service": await self.adapter.get_service_status(adapter), 'rest' : adapterdata }
                if 'url' in adapterdata and self.dataset.nativeDevices['adapters'][adapter]['url']=="":
                    self.dataset.nativeDevices['adapters']
                    predefined=self.loadJSON('api_consumers')
                    predefined[adapter]['url']=adapterdata['url']
                    self.saveJSON('api_consumers',predefined)
                await self.dataset.ingest({'adapters': { adapter : adapterstate }})
        except:
            self.log.info('!! Error getting adapter status after discovery: %s' % adapter, exc_info=True)


    async def discover_activated_adapter(self, adaptername, url, token):
        
        discovery_directive={
                                "directive": {
                                    "header": {
                                        "namespace": "Alexa.Discovery", 
                                        "name": "Discover", 
                                        "messageId": str(uuid.uuid1()),
                                        "payloadVersion": "3"
                                    },
                                    "payload": {
                                        "scope": {
                                            "type": "BearerToken",
                                            "token": "fake_temporary"
                                        }
                                    }
                                }
                            }
                            
        addorupdate=    {   
                            "event": {
                                "header": {
                                    "namespace": "Alexa.Discovery",
                                    "name": "AddOrUpdateReport",
                                    "payloadVersion": "3",
                                    "messageId": str(uuid.uuid1())
                                },
                                "payload": {
                                    "endpoints": []
                                }
                            }
                        }
      
        try:
            disc_start=datetime.datetime.now()
            data=json.dumps(discovery_directive)
            adapter_response=await self.adapter_post(url, discovery_directive, token=token)

            endpoints=[]
            try:
                new_devs=[]
                endpoints=adapter_response['event']['payload']['endpoints']
                addorupdate["event"]["payload"]["endpoints"]=endpoints
                await self.adapter.handleAddOrUpdateReport(addorupdate)

                # device_adapter_map is only necessary for an event_gateway/hub adapter to track which messages 
                # go to which adapter directly
                new_map_items=False
                for item in endpoints:
                    if item['endpointId'] not in self.device_adapter_map or self.device_adapter_map[item['endpointId']]!=adaptername:
                        new_map_items=True
                        self.device_adapter_map[item['endpointId']]=adaptername
                    new_devs.append(item['endpointId'])
                    
                if new_map_items:
                    self.adapter.save_cache('adapter_map', self.device_adapter_map)
                
                # TODO/CHEESE 10/7/20 Should this be a manual remove process by the user instead?  It's designed to clean up
                # old devices, but might instead cause churn if there are 'slow add' devices in an adapter.
                # removed temporarily for testing
                
                # This removes devices that are no longer supported by a specific adapter.
                # await self.adapter.remove_devices(await self.check_adapter_devices(adaptername, new_devs))
            except KeyError:
                self.log.error('!! Malformed discovery response: %s %s' % (adaptername, adapter_response))

            disc_end=datetime.datetime.now()
            if len(endpoints)>0:
                self.log.info('>> %s - discovered %s devices @ %s in %s seconds' % (adaptername, len(endpoints),  url, (disc_end-disc_start).total_seconds()) )
                if adaptername in self.adapters_not_responding:
                    self.log.info('.. removing %s from not-responding adapters list: %s' % (adaptername, self.adapters_not_responding))
                    self.adapters_not_responding.remove(adaptername)
                    await self.adapter.virtual_device_refresh(adaptername)
                await self.queue_initial_state_reports(adaptername, endpoints)
            else:
                self.log.info('>| %s - discovered 0 devices' % (adaptername))
                    
        except:
            self.log.error('!! Error running discovery on adapter: %s %s' % (adaptername, url) ,exc_info=True)


    def ReportState(self, endpointId, correlationToken=None , bearerToken=''):

        if not correlationToken:
            correlationToken=str(uuid.uuid1())

        report_state_directive={
            "directive": {
                "header": {
                    "name":"ReportState",
                    "payloadVersion": "3",
                    "messageId": str(uuid.uuid1()),
                    "namespace": "Alexa",
                    "correlationToken": correlationToken,
                },
                "endpoint": {
                    "endpointId": endpointId,
                    "scope": {
                        "type": "BearerToken",
                        "token": bearerToken
                    },     
                    "cookie": {}
                },
                "payload": {}
            },
        }
        
        return report_state_directive

            
    async def queue_initial_state_reports(self, adapter_name, endpoints):
        try:
            tasks=[]
            queued_for_state=[]
            semaphore = asyncio.BoundedSemaphore(10)
            for endpoint in endpoints:  # Identify devices with retrievable properties
                if endpoint['endpointId'] not in queued_for_state:
                    for cap in endpoint['capabilities']:
                        try:
                            if 'properties' in cap and cap['properties']['retrievable']:
                                queued_for_state.append(endpoint['endpointId'])
                                tasks.append(asyncio.ensure_future(self.get_initial_state_report(adapter_name, endpoint['endpointId'], semaphore)))
                                break
                        except:
                            self.log.error('!! Error checking cap %s' % cap ,exc_info=True)      
            responses=await asyncio.gather(*tasks)
            self.log.info('>] %s - cached state for %s/%s devices' % (adapter_name, responses.count(1), len(responses)))
        except:
            self.log.error('!! Error queue_initial_state_reports' ,exc_info=True)      


    async def get_initial_state_report(self, adapter_name, endpointId, semaphore):
        # new: only enter if semaphore can be acquired
        async with semaphore:
            try:
                adapter_response=""
                url=self.adapters[adapter_name]['url']
                response_token=self.adapters[adapter_name]['response_token']
                report_state=self.ReportState(endpointId)
                adapter_response=await self.adapter_post(url, report_state, token=response_token)
                
                if adapter_response and adapter_response['event']['header']['name']=='StateReport':
                    await self.adapter.handleStateReport(adapter_response, source=adapter_name)
                    return True
            except:
                self.log.info('!! error caching initial state for %s %s' % (endpointId, adapter_response), exc_info=True)
            return False
            

    async def check_adapter_devices(self, adaptername, new_devs):
        try:
            remove_devs=[]
            for dev in dict(self.device_adapter_map):
                if self.device_adapter_map[dev]==adaptername:
                    if dev not in new_devs:
                        if dev in self.dataset.devices:
                            self.log.info('.. removing device: %s' % self.dataset.devices[dev])
                            remove_devs.append(dev)
                            del self.device_adapter_map[dev]
                        else:
                            self.log.info('.. removing device not in devices: %s' % dev)
                        
            if remove_devs:
                self.log.info('.. removing devices: %s' % remove_devs)
                del_report=await self.dataset.generateDeleteReport(remove_devs)
                await self.adapter.handleDeleteReport(del_report)
                
            return remove_devs

        except:
            self.log.error('!! Error checking other devs' ,exc_info=True)


    async def activation_refresh_handler(self, request):
        try:
            body=await request.read()
            data=json.loads(body.decode())
            #self.log.info('.. activation request for %s' % data['name'])
            check=await self.auth.get_token_from_api_key(data['name'], data['api_key'])
            if check:
                expiration=datetime.datetime.now() + datetime.timedelta(seconds=(self.config.token_expires-5))
                return self.json_response({'token': check, 'expiration': expiration.isoformat() })

        except:
            self.log.error('!! error with activation request', exc_info=True)
        return self.json_response({'refresh': 'failed'})


    async def list_activations(self):
        try:
            self.log.info('.. activations list request')
            obscured_keys=[]
            for pender in self.pending_activations:
                obscured_keys.append( { "name":pender['name'], "key": pender["api_key"][-6:] } )
                
            activated_keys=[]
            for approved in api_consumer.objects.all():
                activated_keys.append({ "name":approved.name, "key": approved.api_key[-6:]})
                self.log.info('item: %s' % approved)
                
            return {"pending": obscured_keys, "activated": activated_keys}
        except:
            self.log.error('!! error with activations list', exc_info=True)
        return []


    async def activations_handler(self, request):
        try:
            return self.json_response(await self.list_activations())
        except:
            self.log.error('!! error with activations list handler', exc_info=True)
        return self.json_response([])


    async def status_handler(self, request):
        try:
            return self.json_response({"status": "up"})
        except:
            self.log.error('!! error with status handler', exc_info=True)
        return self.json_response([])


    async def status_post_handler(self, request):
          
        result={} 
        if request.body_exists:
            try:
                result={}
                outputData={}
                body=await request.read()
                body=body.decode()
                self.log.info('list post request: %s' % (body))
                #item="%s?%s" % (request.match_info['list'], request.query_string)
                item=request.match_info['list']
                source=item.split('/',1)[0] 
                if source in self.adapters:
                    result='#'
                    url = 'http://%s:%s/list/%s' % (self.adapters[source]['address'], self.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('>> Posting list request to %s %s' % (source, item.split('/',1)[1] ))
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)
        
        return self.json_response(result)


    async def activation_approve_handler(self, request):
        
        # This handler allows an admin to approve/remove new adapters and api keys.
        
        try:
            cmd=request.match_info['cmd']
            remove_pending=None
            body=await request.read()
            data=json.loads(body.decode())
            self.log.info('cmd: %s' % cmd)

            if cmd=='approve':
                self.log.info('.. activation approval for %s' % data['name'])
                for pender in self.pending_activations:
                    if pender['name']==data['name'] and pender['api_key'][-6:]==data['api_key']:
                        remove_pending=pender
                        api_consumer.objects.create(name=pender['name'], api_key=pender['api_key'])
                        self.api_consumers[pender['name']]={ "api_key": pender['api_key'] }
                        self.adapter.saveJSON('api_consumers',self.api_consumers)
                        break
                if remove_pending:
                    self.pending_activations.remove(remove_pending)
                    #return self.json_response({'activation': 'approved', "name":data['name'], "short_key":data['api_key']})

            if cmd=='remove':
                self.log.info('.. activation removal for %s' % data['name'])
                if data['name'] in self.api_consumers:
                    api_consumer.objects.delete(name=data['name'])
                    self.adapter.saveJSON('api_consumers',self.api_consumers)
                    
        except:
            self.log.error('!! error with activation approval request', exc_info=True)
        return self.json_response(await self.list_activations())

        
    async def logout_handler(self, request):
        return self.json_response({"loggedIn":False})  

            

    async def directivesHandler(self, request):
        try:
            #await check_permission(request, 'api')
            directives=await self.dataset.getAllDirectives()
            return self.json_response(directives)
        except:
            self.log.error('Error with Directives Handler', exc_info=True)
            return self.json_response({'Error':True})

    async def eventsHandler(self, request):
        
        eventSources={ 'DoorbellEventSource': { "event": "doorbellPress"}}
        return self.json_response(eventSources)
        
        
    async def propertiesHandler(self, request):
        
        properties=await self.dataset.getAllProperties()
        return self.json_response(properties)
        

    async def listHandler(self, request):

        try:
            adapter_response={}
            item="%s?%s" % (request.match_info['list'], request.query_string)
            item=request.match_info['list']
            
            if item=="devices":
                return self.json_response(self.dataset.devices)
            
            adapter_name=item.split('/',1)[0]
            if adapter_name in self.adapters:
                url = '%s/list/%s' % (self.adapters[adapter_name]['url'], item.split('/',1)[1] )
                response_token=self.adapters[adapter_name]['response_token']
                adapter_response=await self.adapter_post(url, token=response_token, method='GET')
            else:
                self.log.error('>! %s - list request for %s (unavailable)' % (request.api_consumer, adapter_name))
            return self.json_response(adapter_response)

        except aiohttp.client_exceptions.ClientConnectorError:
            self.log.error('!! Connection refused for adapter %s.  Adapter is likely stopped' % adapter_name)
        except ConnectionRefusedError:
            self.log.error('!! Connection refused for adapter %s.  Adapter is likely stopped' % adapter_name)
        except concurrent.futures._base.TimeoutError:
            self.log.error('!! Error getting list data %s (timed out)' % item)
        except concurrent.futures._base.CancelledError:
            self.log.error('!! Error getting list data %s (cancelled)' % item)
        except:
            self.log.error('!! Error getting list data %s %s' % (request.__dict__, item), exc_info=True)
        
        return self.json_response({})
        

    async def listPostHandler(self, request):
          
        result={} 
        if request.body_exists:
            try:
                result={}
                outputData={}
                body=await request.read()
                body=body.decode()
                item=request.match_info['list']
                source=item.split('/',1)[0] 
                self.log.info('>> %s list post request: %s' % (request.api_consumer, item))
                if source in self.adapters:
                    result='#'
                    url = '%s/list/%s' % (self.adapters[source]['url'], item.split('/',1)[1] )
                    #url = 'http://%s:%s/list/%s' % (self.adapters[source]['address'], self.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('<< Posting list request to %s %s' % (source, item.split('/',1)[1] ))
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
                            if type(result)==str:
                                result=json.loads(result)
            except:
                self.log.error('!! Error with list request: %s' % body,exc_info=True)
        
        return self.json_response(result)


    async def imageGetter(self, item, width=640, thumbnail=False):

        try:
            reqstart=datetime.datetime.now()
            source=item.split('/',1)[0] 
            if source in self.adapters:
                result='#'
                if thumbnail:
                    url = '%s/thumbnail/%s' % (self.adapters[source]['url'], item.split('/',1)[1] )
                else:
                    url = '%s/image/%s' % (self.adapters[source]['url'], item.split('/',1)[1] )
                async with aiohttp.ClientSession() as client:
                    async with client.get(url) as response:
                        result=await response.read()
                        if "{" in str(result)[:10]:
                            self.log.info('.. error getting image %s: %s' % (item, result.decode()))
                        return result
                        #result=result.decode()
                        if str(result)[:10]=="data:image":
                            #result=base64.b64decode(result[23:])
                            self.imageCache[item]=str(result)
                            return result

        except aiohttp.client_exceptions.ClientOSError:
            self.log.warning('.. image request failed after %s seconds for %s (Client Error/Connection Reset)' % ((datetime.datetime.now()-reqstart).total_seconds(), item))
        except concurrent.futures._base.CancelledError:
            self.log.warning('.. image request cancelled after %s seconds for %s' % ((datetime.datetime.now()-reqstart).total_seconds(), item))
        except:
            self.log.error('Error after %s seconds getting image %s' % ((datetime.datetime.now()-reqstart).total_seconds(), item), exc_info=True)
        return None


    async def imageHandler(self, request):

        try:
            fullitem="%s?%s" % (request.match_info['item'], request.query_string)
            #self.log.info('.. image request: %s (%s)' % (fullitem, request.query_string))
            if fullitem in self.imageCache:
                self.log.info('.. image from cache')
                result=base64.b64decode(self.imageCache[fullitem][23:])
                return aiohttp.web.Response(body=result, headers = { "Content-type": "image/jpeg" })
            
            if "width=" not in request.query_string or request.path.find('thumbnail')>0:
                result=await self.imageGetter(fullitem, thumbnail=True)
            else:
                result=await self.imageGetter(fullitem)
            
            return aiohttp.web.Response(body=result, headers = { "Content-type": "image/jpeg" })
            
            if str(result)[:10]=="data:image":
                result=base64.b64decode(result[23:])
                #result=base64.b64decode(result[23:])
                return aiohttp.web.Response(body=result, headers = { "Content-type": "image/jpeg" })
            
            self.log.info('Did not get an image to return for %s: %s' % (request.match_info['item'], str(result)[:10]))
            return aiohttp.web.Response(content_type="text/html", body='')
        except:
            self.log.error('Error with image handler', exc_info=True)


    async def adapterSaveHandler(self, request):

        try:    
            if request.body_exists:
                try:
                    outputData={}
                    body=await request.read()
                    #item="%s?%s" % (request.match_info['save'], request.query_string)
                    item=request.match_info['save']
                    self.log.info('save: %s %s' % (item, body))
                    source=item.split('/',1)[0] 
                    if source in self.adapters:
                        result='#'
                        url = '%s/save/%s' % (self.adapters[source]['url'], item.split('/',1)[1] )
                        async with aiohttp.ClientSession() as client:
                            async with client.post(url, data=body) as response:
                                result=await response.read()
                                result=result.decode()
                                self.log.info('resp: %s' % result)
                    
                except:
                    self.log.error('Error transferring command: %s' % body,exc_info=True)
    
                return aiohttp.web.Response(text=result)
        except:
            self.log.error('Error with save command',exc_info=True)


    async def adapterDelHandler(self, request):
            
        try:
            outputData={}   
            body=""
            if request.body_exists:
                body=await request.read()
            #item="%s?%s" % (request.match_info['del'], request.query_string)
            item=request.match_info['del']
            source=item.split('/',1)[0] 
            if source in self.adapters:
                result='#'
                url = 'http://%s:%s/del/%s' % (self.adapters[source]['address'], self.adapters[source]['port'], item.split('/',1)[1] )
                self.log.info('Posting Delete Data to: %s' % url)
                async with aiohttp.ClientSession() as client:
                    async with client.post(url, data=body) as response:
                        result=await response.read()
                        result=result.decode()
                        self.log.info('resp: %s' % result)
            
        except:
            self.log.error('Error transferring command: %s' % body,exc_info=True)

        return aiohttp.web.Response(text=result)


    async def adapterAddHandler(self, request):
        if request.body_exists:
            try:
                outputData={}
                body=await request.read()
                #item="%s?%s" % (request.match_info['add'], request.query_string)
                item=request.match_info['add']
                source=item.split('/',1)[0] 
                if source in self.adapters:
                    result='#'
                    url = 'http://%s:%s/add/%s' % (self.adapters[source]['address'], self.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('Posting Add Data to: %s' % url)
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
                            self.log.info('resp: %s' % result)
                
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)

            return aiohttp.web.Response(text=result)


    async def hub_discovery(self, adapter_name, categories):
    
        # respond to discovery with list of local devices
        
        disco=[]
        try:
            endpoints=await self.dataset.discovery()
            locals=endpoints['event']['payload']['endpoints']
            for dev in locals:
                if type(dev)==str:
                    self.log.info('%s' % dev)
                #if not self.dataset.devices[dev].hidden:
                if 'ALL' in categories or any(item in dev['displayCategories'] for item in categories):
                    disco.append(dev)
    
    
            for dev in self.dataset.devices:
                #if not self.dataset.devices[dev].hidden:
                if 'ALL' in categories or any(item in self.dataset.devices[dev]['displayCategories'] for item in categories):
                    disco.append(self.dataset.devices[dev])
                    
            self.log.info('<< %s Discover.Response %s devices filtered for %s' % (adapter_name, len(disco), categories) )
        except:
            self.log.error('!! error in hub_discovery', exc_info=True)
            
        return {
            "event": {
                "header": {
                    "namespace": "Alexa.Discovery",
                    "name": "Discover.Response",
                    "payloadVersion": "3",
                    "messageId":  str(uuid.uuid1()),
                },
                "payload": {
                    "endpoints": disco
                }
            }
        }


    # Event Gateway

    async def event_gateway_handler(self, request):
        
        response={}
        try:
            if request.body_exists:
                body=await request.read()
                data=json.loads(body.decode())
                if type(data)==str:
                    data=json.loads(data)
                source=request.api_consumer
                
                response=await self.process_gateway_message(data, source)
                return web.Response(text=json.dumps(response, default=self.date_handler))
            else:
                self.log.info('<< eg no body %s' % request)
        except:
            self.log.error('Error with event gateway',exc_info=True)
        return self.json_response({})
        
        
    async def process_gateway_message(self, message, source):
        
        #  Processes message from the event gateway handler in the web server 
    
        try:
            if 'directive' in message:
                if message['directive']['header']['name']=='Discover':
                    return await self.hub_discovery(source, self.adapters[source]['categories'])
                
                return await self.process_gateway_directive(message, source=source)
                
            if 'event' in message:
                # Everything that isn't a directive gets processed in an async task
                self.log.debug('<< eventGateway %s' % self.dataset.alexa_json_filter(message))
                asyncio.create_task(self.process_gateway_event(message, source=source))
        
        except:
            self.log.error('Error with event gateway',exc_info=True)

        return {}


    async def process_gateway_directive(self, data, source=None):
        try:
            if data['directive']['header']['name'] in ['CheckGroup','ReportStates']:
                endpointId=data['directive']['payload']['endpoints'][0]
            else:
                endpointId=data['directive']['endpoint']['endpointId']
                if endpointId=="pc1:pc:pc1":
                    self.log.info('PC1: %s' % data)
                
            try:
                adapter_name=self.device_adapter_map[endpointId]
            except:
                adapter_name=endpointId.split(":")[0]
            
            if data['directive']['header']['name']=='TurnOn':
                # Intercept Wake on LAN for devices with that controller
                # This should return the deferred result and all of the Alexa model but for now just adds the WOL action
                if endpointId in self.adapter.cached_wol_endpoints:
                    self.log.info('.. triggering WOL for TurnOn directive on device with WakeOnLANController: %s' % endpointId)
                    wol_response = await self.adapter.wake_on_lan(endpointId)
                elif self.dataset.deviceHasCapability(endpointId, 'WakeOnLANController'):
                    self.log.info('.. triggering WOL for TurnOn directive on device with WakeOnLANController: %s' % endpointId)
                    wol_response = await self.adapter.wake_on_lan(endpointId)
                    #return web.Response(text=json.dumps(wol_response, default=self.date_handler))
                    
            if adapter_name=="hub" and endpointId.split(':')[1]=='virtual' and data['directive']['header']['name'] not in ['ReportState','CheckGroup','ReportStates']:
                # Handle directives for virtual composite devices hosted by Hub.  This should get the real controller
                # values from the actual adapters as needed.
                
                #self.log.info('Locals: %s' % self.dataset.localDevices)
                local=self.dataset.localDevices[endpointId]
                controller=getattr(local, data['directive']['header']['namespace'].split('.')[1])
                    
                new_directive=copy.deepcopy(data)
                real_endpoint=controller.real_endpointId
                url=self.adapters[controller.real_adapter]['url']
                response_token=self.adapters[controller.real_adapter]['response_token']
                new_directive['directive']['endpoint']['endpointId']=controller.real_endpointId
                adapter_response=await self.adapter_post(url, new_directive, token=response_token)
                
                updates={}
                if 'context' in adapter_response:
                    for item in adapter_response['context']['properties']:
                        if item['name'] in controller.props:
                            updates[item['name']]=item['value']
                    if updates:
                        self.log.info('.. updating virtual cache: %s %s' % (endpointId.split(':')[2], updates ))
                        await self.adapter.dataset.ingest( { 'virtual' : { endpointId.split(':')[2]: { "cached":  updates }}})
                        
                    virtual_state=local.StateReport()['context']['properties']
                    adapter_response['context']['properties']=virtual_state
                    
                adapter_response['event']['endpoint']['endpointId']=endpointId
                return adapter_response
                
            elif adapter_name=="hub":
                # Handle directives for local hub devices, mostly adapter restarts
                response= await self.dataset.handle_local_directive(data)
                return response

            elif adapter_name in self.adapters:
                # Pass along directives to other adapters as necessary
                if data['directive']['header']['name']=='ReportState':
                    if endpointId in self.adapter.state_cache:
                        correlationToken=data['directive']['header']['correlationToken']
                        return await self.adapter.cached_state_report(endpointId, correlationToken=correlationToken)

                if adapter_name in self.adapters_not_responding:
                    self.log.warning('!< warning: %s requested data from non-responsive adapter: %s' % (source, adapter_name))
                    self.log.warning(devices.ErrorResponse(endpointId, 'BRIDGE_UNREACHABLE', 'requested data from non-responsive adapter'))
                    return devices.ErrorResponse(endpointId, 'BRIDGE_UNREACHABLE', 'requested data from non-responsive adapter')
                url=self.adapters[adapter_name]['url']
                response_token=self.adapters[adapter_name]['response_token']
                adapter_response=await self.adapter_post(url, data, token=response_token)

                # cache result - should be refactored to not compete with handleStateReport
                try:
                    if adapter_response['event']['header']['name']=='StateReport':
                        endpointId=adapter_response['event']['endpoint']['endpointId']
                        self.adapter.state_cache[endpointId]=adapter_response['context']['properties']
                except KeyError:
                    pass

                if data['directive']['header']['name']=='StateReport':
                    await self.adapter.handleStateReport(message, source=source)
                return adapter_response
            else:
                # This is a shim for the non-Alexa ReportStates (multi-report states)
                if data['directive']['header']['name'] =='ReportStates':
                    self.log.warning('!! REPORTSTATES: %s' % data['directive']['payload']['endpoints'])
                    errors={}
                    for dev in data['directive']['payload']['endpoints']:
                        errors[dev]=devices.ErrorResponse(dev, 'NO_SUCH_ENDPOINT', 'hub could not locate this device')
                    return errors
                    
                if endpointId in self.dataset.devices:
                    return devices.ErrorResponse(endpointId, 'BRIDGE_UNREACHABLE', 'requested data from non-responsive adapter')
                    
                return devices.ErrorResponse(endpointId, 'NO_SUCH_ENDPOINT', 'hub could not locate this device')
            
        except:
            self.log.error('!! error finding endpoint', exc_info=True)
            
        return devices.ErrorResponse(endpointId, 'INTERNAL_ERROR', 'hub failed to process gateway directive')


    async def process_gateway_event(self, message, source=None):
            
        try:
            endpointId=message['event']['endpoint']['endpointId']
            if endpointId=="pc1:pc:pc1":
                self.log.info('PC1: %s' % message)

            if 'correlationToken' in message['event']['header']:
                try:
                    if message['event']['header']['correlationToken'] in self.pendingRequests:
                        self.pendingResponses[message['event']['header']['correlationToken']]=message
                        self.pendingRequests.remove(message['event']['header']['correlationToken'])
                except:
                    self.log.error('Error handling a correlation token response: %s ' % message, exc_info=True)

            elif message['event']['header']['name']=='DoorbellPress':
                if message['event']['endpoint']['endpointId'].split(":")[0]!=self.dataset.adaptername:
                    if hasattr(self.adapter, "handleAlexaEvent"):
                        await self.adapter.handleAlexaEvent(message, source=source)
        
            elif message['event']['header']['name']=='StateReport':
                #if message['event']['endpoint']['endpointId'].split(":")[0]!=self.dataset.adaptername:
                if hasattr(self.adapter, "handleStateReport"):
                    await self.adapter.handleStateReport(message, source=source)

            elif message['event']['header']['name']=='ChangeReport':
                #self.log.info('Change report: %s' % message)
                #if message['event']['endpoint']['endpointId'].split(":")[0]!=self.dataset.adaptername:
                if hasattr(self.adapter, "handleChangeReport"):
                    await self.adapter.handleChangeReport(message, source=source)

            elif message['event']['header']['name']=='DeleteReport':
                if hasattr(self.adapter, "handleDeleteReport"):
                    await self.adapter.handleDeleteReport(message, source=source)
            
            elif message['event']['header']['name']=='AddOrUpdateReport':
                if hasattr(self.adapter, "handleAddOrUpdateReport"):
                    await self.adapter.handleAddOrUpdateReport(message, source=source)
                    #await self.adapter.handleAddOrUpdateReport(message['event']['payload']['endpoints'])
            
            else:
                self.log.warning('.! gateway message type not processed: %s' % message)
                    
        except:
            self.log.error('!! Error processing gateway message: %s' % message, exc_info=True)

    # New message distribution system that scans events and forwards them via rest to collector adapters
    
    async def add_collector_update(self, message, source=None):
        try:
            for adapter in self.adapters:
                if not self.adapters[adapter]['collector']:
                    continue
                if len(self.adapters[adapter]['categories'])==0:
                    continue
                device={}
                try:
                    if message['event']['header']['name']=='AddOrUpdateReport':
                        adapter_endpoints=[]
                        for item in message['event']['payload']['endpoints']:
                            device=self.dataset.getDeviceByEndpointId(item['endpointId'])    
                            if 'ALL' in self.adapters[adapter]['categories'] or any(item in device['displayCategories'] for item in self.adapters[adapter]['categories']):
                                adapter_endpoints.append(item)
                        if len(adapter_endpoints)>0:
                            #self.log.info('>> Adding %s devices to %s' % (len(adapter_endpoints), adapter))
                            updated_message=dict(message)
                            updated_message['event']['payload']['endpoints']=adapter_endpoints
                            asyncio.create_task(self.adapter_post(self.adapters[adapter]['url'], updated_message, token=self.adapters[adapter]['response_token']))

                    elif message['event']['header']['name']=='DeleteReport':
                        adapter_endpoints=[]
                        for item in message['event']['payload']['endpoints']:
                            device=self.dataset.getDeviceByEndpointId(item['endpointId'])    
                            if 'ALL' in self.adapters[adapter]['categories'] or any(item in device['displayCategories'] for item in self.adapters[adapter]['categories']):
                                adapter_endpoints.append(item)
                        if len(adapter_endpoints)>0:
                            #self.log.info('>> Adding %s devices to %s' % (len(adapter_endpoints), adapter))
                            updated_message=dict(message)
                            updated_message['event']['payload']['endpoints']=adapter_endpoints
                            asyncio.create_task(self.adapter_post(self.adapters[adapter]['url'], updated_message, token=self.adapters[adapter]['response_token']))
                    
                    elif 'endpoint' in message['event']:
                        if source==adapter:  # This check has to bypass AddOrUpdate to avoid device list gaps where local devices don't get added 
                            continue
                        device=self.dataset.getDeviceByEndpointId(message['event']['endpoint']['endpointId'], as_dict=True)
                        if not device:
                            self.log.warning('.. warning: did not find device for %s : %s' % (message['event']['endpoint']['endpointId'], message['event']['header']['name']))
                            break
                        else:
                            try:
                                #if 'ALL' in self.adapters[adapter]['categories'] or any(item in device.displayCategories for item in self.adapters[adapter]['categories']):
                                if 'ALL' in self.adapters[adapter]['categories'] or any(item in device['displayCategories'] for item in self.adapters[adapter]['categories']):
                                    asyncio.create_task(self.adapter_post(self.adapters[adapter]['url'], message, token=self.adapters[adapter]['response_token']))
                            except:
                                self.log.error('!! error adding collector update - %s v %s' % (self.adapters[adapter],device ), exc_info=True)
                    else:
                        self.log.warning('.! warning: no endpoint in %s' % message)
                    
                except:
                    self.log.error('!! error with acu: %s' % adapter, exc_info=True)

        except concurrent.futures._base.CancelledError:
            self.log.error('Error updating collectors (cancelled)', exc_info=True)
        except:
            self.log.error('Error updating collectors', exc_info=True)


    async def remove_adapter_activation_by_url(self, url, token):
        try:
            all_adapters=self.adapters
            for adapter in all_adapters:
                if all_adapters[adapter]['url']==url and self.adapters[adapter]['response_token']==token:
                    self.log.info('.. retracting bad adapter activation: %s' % self.adapters[adapter]['name'])
                    del self.adapters[adapter]
                    break
        except:
            self.log.error("!. Error removing adapter activation for %s" % url, exc_info=True)
            
            
    async def adapter_post(self, url, data={}, headers={ "Content-type": "text/xml" }, token=None, adapter='', method="POST"):  
        
        try:
            if self.adapter_name_from_url(url) in self.adapters_not_responding:
                self.log.error("!. Error - Request for adapter that is offline: %s %s %s" % (self.adapter_name_from_url(url), url, headers))
                return {}        
                
            if not token:
                self.log.error('!! error - no token provided for %s %s' % (url, data))
                return {}

            if not url:
                self.log.error('!! Error sending rest/post - no URL for %s' % data)
                return {}
                
            headers['authorization']=token
            timeout = aiohttp.ClientTimeout(total=self.restTimeout)
            jsondata=json.dumps(data)
            
            #self.log.info('Adapter post: %s %s' % (url, data))
            
            async with aiohttp.ClientSession(connector=self.conn, connector_owner=False, timeout=timeout) as client:
                poststart=datetime.datetime.now()
                if method=='POST':
                    response=await client.post(url, data=jsondata, headers=headers)
                elif method=='GET':
                    response=await client.get(url, headers=headers)
                else:
                    self.log.error('!! unknown method: %s' % method)
                    return {}

                postend=datetime.datetime.now()
                if response.status == 200:
                    #self.log.info('>> event gateway: %s' % (self.adapter.alexa_json_filter(data)))
                    if self.adapter_name_from_url(url) in self.adapters_not_responding:
                        self.log.info('.. removing adapter %s on status code %s from not responding list: %s' % (self.adapter_name_from_url(url), response.status, self.adapters_not_responding))
                        self.adapters_not_responding.remove(self.adapter_name_from_url(url))
                        
                    result=await response.read()
                    #self.log.info('adapter response: %s' % result)
                elif response.status == 401:
                    all_adapters=self.adapters
                    await self.remove_adapter_activation_by_url(url, token)
                    self.log.info('.. error response token is not valid: %s %s %s' % (adapter, response.status, url))
                    return {}
                elif response.status == 500:
                    #await self.remove_adapter_activation_by_url(url, token)
                    self.log.error('>! Error sending to %s %s (500 remote adapter internal error)' % (adapter, url))
                    return {}
                else:
                    #await self.remove_adapter_activation_by_url(url, token)
                    self.log.error('>! Error sending to adapter: %s' % response.status)
                    return {}

                try:
                    posttime=(postend-poststart).total_seconds()
                    if posttime>self.slow_post:
                        self.log.info('.! slow adapterpost: %s %s / datasize: %s<>%s / runtime: %s / %s ' % (self.adapter_name_from_url(url), url, len(jsondata), len(result), posttime, self.dataset.alexa_json_filter(data)))
                    if len(result.decode())==0:
                        return {}
                    jsonresult=json.loads(result.decode())
                    #jsonresult['adapterpost']={ "url": url, "runtime": (datetime.datetime.now()-reqstart).total_seconds() }
                    return jsonresult
                except:
                    self.log.info('.. bad json response: %s' % result, exc_info=True)
                    self.log.info('.. request was: %s' % data)

                return {}
        
        except concurrent.futures._base.TimeoutError:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error("!. Error - Timeout in rest post to %s: %s %s" % (self.adapter_name_from_url(url), url, headers))
        except concurrent.futures._base.CancelledError:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error("!. Error - Cancelled rest post to %s: %s %s %s" % (self.adapter_name_from_url(url), url, headers, self.dataset.alexa_json_filter(data)))
        except aiohttp.client_exceptions.ClientConnectorError:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error("!. Error - adapter post failed to %s %s %s" % (self.adapter_name_from_url(url), url, self.dataset.alexa_json_filter(data)))
        except aiohttp.client_exceptions.ClientOSError:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error("!. Error - adapter post failed to %s %s %s (Client OS Error / Connection reset by peer)" % (self.adapter_name_from_url(url), url, self.dataset.alexa_json_filter(data)))
        except ConnectionRefusedError:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error('!. Connection refused for adapter %s %s. %s %s' % (self.adapter_name_from_url(url), url, data, str(e)))     
        except:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error("!. Error requesting state: %s" % data,exc_info=True)
                
        if self.adapter_name_from_url(url) not in self.adapters_not_responding:
            self.log.warning('!+ adapter %s added to not responding list' % self.adapter_name_from_url(url) )
            self.adapters_not_responding.append(self.adapter_name_from_url(url))
            await self.adapter.check_adapter_health(self.adapter_name_from_url(url))
            
        return {}
       
        
    def adapter_name_from_url(self, url):
        try:
            for adp in self.adapters:
                if self.adapters[adp]['url']==url:
                    return adp
        except:
            self.log.error('!. error getting adapter from url %s' % url)
        return None