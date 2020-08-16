#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase, adapterbase, configbase
from sofacollector import SofaCollector

import devices
import json
import asyncio
import aiohttp
from aiohttp import web
import aiohttp_cors
from aiohttp_sse import sse_response

import concurrent.futures
import aiofiles
import datetime
import base64
import ssl
import uuid
import subprocess
import pystemd

from pystemd.systemd1 import Unit

from auth import api_consumer, Auth
            
class sofaHub():
    
    def __init__(self, api_consumers=None, loop=None, log=None, dataset=None, adapter=None, config=None):
        self.config=config
        self.api_consumers=api_consumers
        self.log=log
        self.loop = loop
        self.adapter=adapter
        self.dataset=dataset
        self.imageCache={}
        self.layout={}
        self.adapterTimeout=2
        self.slow_post=2
        self.sse_updates=[]
        self.sse_last_update=datetime.datetime.now(datetime.timezone.utc)
        self.active_sessions={}
        self.pending_activations=[]
        self.device_adapter_map={}
        self.restTimeout=5
        self.conn = aiohttp.TCPConnector()
        self.adapters_not_responding=[]
        
    async def initialize(self):

        try:
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

    async def discover_activated_adapter(self, adaptername, url, token):
        
        discovery_directive={ "directive": {
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
                            
        addorupdate=    {   "event": {
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
            adapter_response=await self.adapterPost(url, discovery_directive, token=token)
            endpoints=[]
            try:
                new_devs=[]
                endpoints=adapter_response['event']['payload']['endpoints']
                await self.adapter.updateDeviceList(endpoints)
                for item in endpoints:
                    self.device_adapter_map[item['endpointId']]=adaptername
                    new_devs.append(item['endpointId'])
                addorupdate["event"]["payload"]["endpoints"]=endpoints
                await self.add_collector_update(addorupdate, source=adaptername)
                await self.adapter.remove_devices(await self.check_adapter_devices(adaptername, new_devs))
            except KeyError:
                self.log.error('!! Malformed discovery response: %s %s' % (adaptername, adapter_response))

            disc_end=datetime.datetime.now()
            if len(endpoints)>0:
                self.log.info('>> Discovered %s devices on %s @ %s in %s seconds' % (len(endpoints), adaptername, url, (disc_end-disc_start).total_seconds()) )

        except:
            self.log.error('!! Error running discovery on adapter: %s %s' % (adaptername, url) ,exc_info=True)


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

    async def activation_handler(self, request):
        try:
            body=await request.read()
            data=json.loads(body.decode())
            #self.log.info('.. activation request for %s' % data['name'])
            check=await self.auth.get_token_from_api_key(data['name'], data['api_key'])
            if check:
                expiration=datetime.datetime.now() + datetime.timedelta(seconds=(self.config.token_expires-5))
                if 'type' in data and data['type']=='adapter':
                    if 'url' in data:
                        self.dataset.adapters[data['name']]=data
                        asyncio.create_task(self.discover_activated_adapter(data['name'], data['url'], data['response_token']))
                    if hasattr(self.adapter, "virtualAddAdapter"):
                        asyncio.create_task(self.adapter.virtualAddAdapter(data['name'], data))               
                self.log.info('.. activated adapter %s until %s' % (data['name'], expiration.isoformat()))
                return self.json_response({'token': check, 'expiration': expiration.isoformat() })
                
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
                if source in self.dataset.adapters:
                    result='#'
                    url = 'http://%s:%s/list/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('>> Posting list request to %s %s' % (source, item.split('/',1)[1] ))
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)
        
        return self.json_response(result)


    async def activation_approve_handler(self, request):
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
        
    async def layoutHandler(self, request):
        self.layout=await self.layoutUpdate()
        return self.json_response(self.layout)      
        
        
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
            if adapter_name in self.dataset.adapters:
                url = '%s/list/%s' % (self.dataset.adapters[adapter_name]['url'], item.split('/',1)[1] )
                response_token=self.dataset.adapters[adapter_name]['response_token']
                adapter_response=await self.adapterPost(url, token=response_token, method='GET')
            else:
                self.log.error('>! list handler request for unknown adapter: %s %s' % (adapter_name, self.dataset.adapters))
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
            self.log.error('!! Error getting list data %s' % item, exc_info=True)
        
        return self.json_response({})
        

    async def listPostHandler(self, request):
          
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
                if source in self.dataset.adapters:
                    result='#'
                    url = '%s/list/%s' % (self.dataset.adapters[source]['url'], item.split('/',1)[1] )
                    #url = 'http://%s:%s/list/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('>> Posting list request to %s %s' % (source, item.split('/',1)[1] ))
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
                            if type(result)==str:
                                result=json.loads(result)
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)
        
        return self.json_response(result)


    async def imageGetter(self, item, width=640, thumbnail=False):

        try:
            reqstart=datetime.datetime.now()
            source=item.split('/',1)[0] 
            if source in self.dataset.adapters:
                result='#'
                if thumbnail:
                    url = '%s/thumbnail/%s' % (self.dataset.adapters[source]['url'], item.split('/',1)[1] )
                else:
                    url = '%s/image/%s' % (self.dataset.adapters[source]['url'], item.split('/',1)[1] )
                async with aiohttp.ClientSession() as client:
                    async with client.get(url) as response:
                        result=await response.read()
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
                    if source in self.dataset.adapters:
                        result='#'
                        url = '%s/save/%s' % (self.dataset.adapters[source]['url'], item.split('/',1)[1] )
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
            if source in self.dataset.adapters:
                result='#'
                url = 'http://%s:%s/del/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
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
                if source in self.dataset.adapters:
                    result='#'
                    url = 'http://%s:%s/add/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('Posting Add Data to: %s' % url)
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
                            self.log.info('resp: %s' % result)
                
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)

            return aiohttp.web.Response(text=result)

    async def hub_discovery(self, categories):
    
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
                    
            self.log.info('.. Returning discovery for %s devices filtered for %s' % (len(disco), categories) )
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


    async def event_gateway_handler(self, request):
        
        # TODO: This should be a generic replacement for the directive handler that accepts any Alexa API formatted command
        # from both user endpoints as well as other adapters

        response={}
        try:
            #self.log.info('.. Tasks/Active tasks count: %s %s' % ( len([task for task in asyncio.Task.all_tasks()]), len([task for task in asyncio.Task.all_tasks() if not task.done()]) ))
            if request.body_exists:
                body=await request.read()
                data=json.loads(body.decode())
                if type(data)==str:
                    data=json.loads(data)
                    
                #self.log.info('<< data: %s %s' % (type(data), data))
                if 'directive' in data:
                    if data['directive']['header']['name']=='Discover':
                        lookup=await self.hub_discovery(self.dataset.adapters[request.api_consumer]['categories'])
                        #self.log.info('>> discovery Devices: %s' % lookup)
                        return web.Response(text=json.dumps(lookup, default=self.date_handler))
                    try:
                        if data['directive']['header']['name'] in ['CheckGroup','ReportStates']:
                            #self.log.info('<< eg directive: %s %s' % (request.user, data))
                            endpointId=data['directive']['payload']['endpoints'][0]
                            #adapter_response=await self.dataset.requestReportStates(data['directive']['payload']['endpoints'])
                            #return web.Response(text=json.dumps(adapter_response, default=self.date_handler))
                        else:
                            endpointId=data['directive']['endpoint']['endpointId']
                            
                        try:
                            adapter_name=self.device_adapter_map[endpointId]
                        except:
                            adapter_name=endpointId.split(":")[0]
                            
                        if adapter_name=="hub":
                            response= await self.dataset.handle_local_directive(data)
                            return web.Response(text=json.dumps(response, default=self.date_handler))
                            
                        elif adapter_name in self.dataset.adapters:
                            url=self.dataset.adapters[adapter_name]['url']
                            response_token=self.dataset.adapters[adapter_name]['response_token']
                            adapter_response=await self.adapterPost(url, data, token=response_token)
                            #self.log.info('>> adapter response: %s' % adapter_response)
                            return web.Response(text=json.dumps(adapter_response, default=self.date_handler))
                    except:
                        self.log.error('!! error finding endpoint', exc_info=True)
                        
                else:
                    directive=data['event']['header']['name']
                    self.log.debug('<< eventGateway %s' % self.dataset.alexa_json_filter(data))
                    asyncio.create_task(self.process_gateway_message(data, source=request.api_consumer))
                    #self.log.info('.. Tasks/Active tasks count: %s %s' % ( len([task for task in asyncio.Task.all_tasks()]), len([task for task in asyncio.Task.all_tasks() if not task.done()]) ))

                    #await self.adapter.process_event(data, source=request.user) does not handle the source parameter hub needs
            else:
                self.log.info('<< eg no body %s' % request)
        except:
            self.log.error('Error with event gateway',exc_info=True)
        return self.json_response({})


    async def process_gateway_message(self, message, source=None):
            
        try:
            #self.log.info('gateway source: %s' % source)
            if 'event' in message:
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
                    if message['event']['endpoint']['endpointId'].split(":")[0]!=self.dataset.adaptername:
                        if hasattr(self.adapter, "handleStateReport"):
                            await self.adapter.handleStateReport(message, source=source)

                elif message['event']['header']['name']=='ChangeReport':
                    if message['event']['endpoint']['endpointId'].split(":")[0]!=self.dataset.adaptername:
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
            for adapter in self.dataset.adapters:
                if not self.dataset.adapters[adapter]['collector']:
                    continue
                if len(self.dataset.adapters[adapter]['categories'])==0:
                    continue
                device={}
                try:
                    if message['event']['header']['name']=='AddOrUpdateReport':
                        adapter_endpoints=[]
                        for item in message['event']['payload']['endpoints']:
                            device=self.dataset.getDeviceByEndpointId(item['endpointId'])    
                            if 'ALL' in self.dataset.adapters[adapter]['categories'] or any(item in device['displayCategories'] for item in self.dataset.adapters[adapter]['categories']):
                                adapter_endpoints.append(item)
                        if len(adapter_endpoints)>0:
                            #self.log.info('>> Adding %s devices to %s' % (len(adapter_endpoints), adapter))
                            updated_message=dict(message)
                            updated_message['event']['payload']['endpoints']=adapter_endpoints
                            asyncio.create_task(self.adapterPost(self.dataset.adapters[adapter]['url'], updated_message, token=self.dataset.adapters[adapter]['response_token']))

                    elif message['event']['header']['name']=='DeleteReport':
                        adapter_endpoints=[]
                        for item in message['event']['payload']['endpoints']:
                            device=self.dataset.getDeviceByEndpointId(item['endpointId'])    
                            if 'ALL' in self.dataset.adapters[adapter]['categories'] or any(item in device['displayCategories'] for item in self.dataset.adapters[adapter]['categories']):
                                adapter_endpoints.append(item)
                        if len(adapter_endpoints)>0:
                            #self.log.info('>> Adding %s devices to %s' % (len(adapter_endpoints), adapter))
                            updated_message=dict(message)
                            updated_message['event']['payload']['endpoints']=adapter_endpoints
                            asyncio.create_task(self.adapterPost(self.dataset.adapters[adapter]['url'], updated_message, token=self.dataset.adapters[adapter]['response_token']))
                    
                    elif 'endpoint' in message['event']:
                        if source==adapter:  # This check has to bypass AddOrUpdate to avoid device list gaps where local devices don't get added 
                            continue
                        device=self.dataset.getDeviceByEndpointId(message['event']['endpoint']['endpointId'])
                        if not device:
                            self.log.warning('.. warning: did not find device for %s : %s' % (message['event']['endpoint']['endpointId'], message['event']['header']['name']))
                            break
                        else:
                            try:
                                if 'ALL' in self.dataset.adapters[adapter]['categories'] or any(item in device['displayCategories'] for item in self.dataset.adapters[adapter]['categories']):
                                    asyncio.create_task(self.adapterPost(self.dataset.adapters[adapter]['url'], message, token=self.dataset.adapters[adapter]['response_token']))
                            except:
                                self.log.error('%s v %s' % (self.dataset.adapters[adapter],device ))
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
            all_adapters=self.dataset.adapters
            for adapter in all_adapters:
                if all_adapters[adapter]['url']==url and self.dataset.adapters[adapter]['response_token']==token:
                    self.log.info('.. retracting bad adapter activation: %s' % self.dataset.adapters[adapter]['name'])
                    del self.dataset.adapters[adapter]
                    break
        except:
            self.log.error("!. Error removing adapter activation for %s" % url, exc_info=True)
            
            
    async def adapterPost(self, url, data={}, headers={ "Content-type": "text/xml" }, token=None, adapter='', method="POST"):  
        
        try:
            if not token:
                self.log.error('!! error - no token provided for %s %s' % (url, data))
                return {}

            if not url:
                self.log.error('!! Error sending rest/post - no URL for %s' % data)
                return {}
                
            headers['authorization']=token
            timeout = aiohttp.ClientTimeout(total=self.restTimeout)
            jsondata=json.dumps(data)
            
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
                    if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                        self.adapters_not_responding.append(self.adapter_name_from_url(url))
                    result=await response.read()
                elif response.status == 401:
                    all_adapters=self.dataset.adapters
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
        except ConnectionRefusedError:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error('!. Connection refused for adapter %s %s. %s %s' % (self.adapter_name_from_url(url), url, data, str(e)))     
        except:
            if self.adapter_name_from_url(url) not in self.adapters_not_responding:
                self.log.error("!. Error requesting state: %s" % data,exc_info=True)
                
        if self.adapter_name_from_url(url) not in self.adapters_not_responding:
            self.adapters_not_responding.append(self.adapter_name_from_url(url))
            
        return {}
       
        
    def adapter_name_from_url(self, url):
        try:
            for adp in self.dataset.adapters:
                if self.dataset.adapters[adp]['url']==url:
                    return adp
        except:
            self.log.error('!. error getting adapter from url %s' % url)
        return None
        

class hub(sofabase):
    
    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.web_address=self.set_or_default("web_address", mandatory=True)
            self.web_port=self.set_or_default("web_port", default=6443)
            self.token_secret=self.set_or_default("token_secret", mandatory=True)
            self.certificate=self.set_or_default("certificate", mandatory=True)
            self.certificate_key=self.set_or_default("certificate_key", mandatory=True)
            self.token_expires=self.set_or_default("token_expires", default=604800)


    class EndpointHealth(devices.EndpointHealth):
        
        @property            
        def connectivity(self):
            if 'service' in self.nativeObject and 'ActiveState' in self.nativeObject['service']:
                if self.nativeObject['service']['ActiveState']=='active':
                    return 'OK'
            return 'UNREACHABLE'

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
                if 'service' in self.nativeObject and 'ActiveState' in self.nativeObject['service']:
                    if self.nativeObject['service']['ActiveState']=='active':
                        return datetime.datetime.fromtimestamp(self.nativeObject['service']['ExecMainStartTimestamp']).isoformat()
                if 'rest' in self.nativeObject and 'startup' in self.nativeObject['rest']:
                    return self.nativeObject['rest']['startup']
            except:
                self.log.error('!! Error getting startup time', exc_info=True)
                
            return ''


    class PowerController(devices.PowerController):

        @property            
        def powerState(self):
            if 'service' in self.nativeObject and 'ActiveState' in self.nativeObject['service']:
                if self.nativeObject['service']['ActiveState']=='active':
                    return 'ON'
            return 'OFF'
            

        async def TurnOn(self, correlationToken='', **kwargs):
            try:
                self.nativeObject['logged']={'ERROR':0, 'INFO':0}
                stdoutdata = subprocess.getoutput("/opt/sofa-server/svc %s" % self.nativeObject['name'])
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
    
    
    class adapterProcess(SofaCollector.collectorAdapter):

        def __init__(self, log=None, loop=None, dataset=None, config=None, **kwargs):
            self.server=None
            self.dataset=dataset
            self.config=config
            self.log=log
            self.loop=loop
            self.dataset.nativeDevices['adapters']={}
            self.poll_time=10


        async def pre_activate(self):
            
            # This adapter does not activate but this defines the point in the timeline for the correct start
            self.log.info('.. loading predefined adapter list')
            await self.add_defined_adapters()
            
        async def start(self):
            self.log.info('.. Starting sofa hub server')
            self.api_consumers=self.loadJSON("api_consumers")
            self.server = sofaHub(config=self.config, api_consumers=self.api_consumers, loop=self.loop, log=self.log, dataset=self.dataset, adapter=self)
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
                        newadapter['service']=await self.loop.run_in_executor(None, self.get_service_status, adapter)
                        await self.dataset.ingest({'adapters': { adapter : newadapter}})
                    except:
                        self.log.error('!! error initializing adapter %s' % adapter, exc_info=True)
                        
                asyncio.create_task(self.poll_loop())
                
            except:
                self.log.error('!! Error populating adapters', exc_info=True)

        async def poll_loop(self):
            while self.running:
                try:
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
                            adapterstate['service']=await self.loop.run_in_executor(None, self.get_service_status, adapter)
                            await self.dataset.ingest({'adapters': { adapter : adapterstate}})
                    except:
                        self.log.info('!! error during adapter check %s' % (adapter), exc_info=True)
                    
            except:
                self.log.error('!! Error listing adapters', exc_info=True)


        def get_service_status(self, adaptername):
            
            # Warning - this process is not Async and needs to be wrapped in an executor to complete without blocking
            # and causing delays in command passing
            
            try:
                unit = Unit(str.encode('sofa-%s.service' % adaptername))
                unit.load()
                unit.Unit.ActiveState
                unit.Unit.SubState
                #unit.Unit.stop()
                processes=unit.Service.GetProcesses()
                mainprocess=""
                for process in processes:
                    if process[1]==unit.Service.MainPID:
                        mainprocess=process[2].decode()
                return {
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


        async def handleStateReport(self, message, source=None):
            try:
                #await super().handleStateReport(message)
                await self.server.add_collector_update(message, source=source)

            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)


        async def handleAddOrUpdateReport(self, message, source=None):
            try:
                
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
                    try:
                        if self.config.log_changes:
                            self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        await self.server.add_collector_update(message, source=source)

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
            
            try:
                if path.split("/")[1]=="adapters":
                    nativeObject=self.dataset.getObjectFromPath(self.dataset.getObjectPath(path))
                    #self.log.info('native: %s %s' % (path,nativeObject))
                    if nativeObject['name'] not in self.dataset.localDevices: 
                        deviceid=path.split("/")[2]
                        device=devices.alexaDevice('hub/adapters/%s' % deviceid, deviceid, displayCategories=['ADAPTER'], adapter=self)
                        device.PowerController=hub.PowerController(device=device)
                        device.AdapterHealth=hub.AdapterHealth(device=device)
                        device.EndpointHealth=hub.EndpointHealth(device=device)
                        return self.dataset.newaddDevice(device) 
            except:
                self.log.error('!! Error defining smart device', exc_info=True)
                return False
                

        async def virtualAddAdapter(self, adapter, adapterdata):
            
            try:
                #self.log.info('.. getting adapter status for %s' % adapter)
                if adapter in self.dataset.nativeDevices['adapters']:
                    adapterstate={ "service": await self.loop.run_in_executor(None, self.get_service_status, adapter), 'rest' : adapterdata }
                    if 'url' in adapterdata:
                        if self.dataset.nativeDevices['adapters'][adapter]['url']=="":
                            self.dataset.nativeDevices['adapters']
                            predefined=self.loadJSON('api_consumers')
                            predefined[adapter]['url']=adapterdata['url']
                            self.saveJSON('api_consumers',predefined)
   
                    await self.dataset.ingest({'adapters': { adapter : adapterstate }})
            except:
                self.log.info('!! Error getting adapter status after discovery: %s' % adapter, exc_info=True)


        async def virtualList(self, itempath, query={}):
            try:
                if itempath=="activations":
                    return self.server.pending_activations
            except:
                self.log.error('Error getting virtual list for %s' % itempath, exc_info=True)
            return {}


if __name__ == '__main__':
    adapter=hub(name='hub')
    adapter.start()
