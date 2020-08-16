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

from auth import User, Auth
            
class sofaWebUI():
    
    def __init__(self, config=None, loop=None, log=None, dataset=None, adapter=None):
        self.config=config
        self.log=log
        self.loop = loop
        self.adapter=adapter
        self.dataset=dataset
        self.imageCache={}
        self.layout={}
        self.adapterTimeout=2
        self.sse_updates=[]
        self.sse_last_update=datetime.datetime.now(datetime.timezone.utc)
        self.active_sessions={}
        self.active_sse={}
        self.pending_activations=[]
        self.video_keepalives={}
        self.image_timeout=5
        self.gateway_conn = aiohttp.TCPConnector()
        self.session_device_filter={}

    async def initialize(self):

        try:
            self.auth=Auth(secret=self.config.token_secret, log=self.log, token_expires=self.config.token_expires)
            self.users=self.adapter.loadJSON("users")
            for user in self.users:
                User.objects.create(login=user, email=self.users[user]['email'], password=self.users[user]['password'], is_admin=self.users[user]['admin'])

            self.serverAddress=self.config.web_address
            self.serverApp = aiohttp.web.Application(middlewares=[self.auth.middleware])
            
            self.serverApp.router.add_get('/sse', self.sse_handler)
            
            self.serverApp.router.add_get('/', self.root_handler)
            self.serverApp.router.add_post('/auth/o2/token', self.refresh_token_post_handler) # This is the URL that Amazon Alexa uses
            self.serverApp.router.add_post('/login', self.login_post_handler)
            self.serverApp.router.add_get('/logout', self.logout_handler)
            self.serverApp.router.add_get('/directives', self.directivesHandler)
            self.serverApp.router.add_get('/events', self.eventsHandler)
            self.serverApp.router.add_get('/layout', self.layoutHandler)
            self.serverApp.router.add_get('/properties', self.propertiesHandler)
            self.serverApp.router.add_get('/user', self.get_user)
            
            self.serverApp.router.add_post('/directive', self.directiveHandler)
            self.serverApp.router.add_get('/devices', self.device_list_handler)
            self.serverApp.router.add_post('/register_devices', self.register_device_handler)
            
            self.serverApp.router.add_get('/list/{list:.+}', self.listHandler)
            self.serverApp.router.add_post('/list/{list:.+}', self.listPostHandler)
            
            self.serverApp.router.add_post('/add/{add:.+}', self.adapterAddHandler)
            self.serverApp.router.add_post('/del/{del:.+}', self.adapterDelHandler)
            self.serverApp.router.add_post('/save/{save:.+}', self.adapterSaveHandler)
            
            self.serverApp.router.add_get('/image/{item:.+}', self.imageHandler)
            self.serverApp.router.add_get('/thumbnail/{item:.+}', self.imageHandler)
            
            self.serverApp.router.add_get('/video/{camera:.+}', self.video_handler)
            
            self.serverApp.router.add_get('/get-user', self.get_user)
            self.serverApp.router.add_post('/save-user', self.save_user)
            
            if os.path.isdir(self.config.client_build_directory):
                self.serverApp.router.add_static('/client', path=self.config.client_build_directory, append_version=True)
                self.serverApp.router.add_static('/fonts', path=self.config.client_build_directory+"/fonts", append_version=True)
            else:
                self.log.error('!! Client build directory does not exist.  Cannot host client until this directory is created and this adapter is restarted')
            
            # Add CORS support for all routes so that the development version can run from a different port
            self.cors = aiohttp_cors.setup(self.serverApp, defaults={
                "*": aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_methods='*', allow_headers="*") })

            for route in self.serverApp.router.routes():
                self.cors.add(route)

            self.runner=aiohttp.web.AppRunner(self.serverApp)
            await self.runner.setup()

            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self.ssl_context.load_cert_chain(self.config.web_certificate, self.config.web_certificate_key)

            self.site = aiohttp.web.TCPSite(self.runner, self.config.web_address, self.config.web_port, ssl_context=self.ssl_context)
            await self.site.start()

        except:
            self.log.error('Error with ui server', exc_info=True)
            
    def login_required(func):
        def wrapper(self, request):
            if not request.user:
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

    async def get_user(self, request):
        for user in self.users:
            if self.users[user]['email']==request.user.email:
                return self.json_response( self.users[user]['data'] )
        return self.json_response({'user': str(request.user)})

    async def save_user(self, request):
        try:
            if request.body_exists:
                for user in self.users:
                    if self.users[user]['email']==request.user.email:
                        body=await request.read()
                        data=json.loads(body.decode())
                        self.users[user]['data']=data
                        self.adapter.saveJSON('users',self.users)
                        return self.json_response( self.users[user]['data'])
        except:
            self.log.error('!! Error saving user data', exc_info=True)
        return self.json_response({'error':'could not save'})


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


    async def layoutUpdate(self):
        try:
            async with aiofiles.open(os.path.join(self.config.data_directory, 'layout.json'), mode='r') as f:
                layout = await f.read()
                return json.loads(layout)
        except concurrent.futures._base.CancelledError:
            self.log.error('!! Error getting layout data (cancelled)')
        except:
            self.log.error('!! Error getting layout data', exc_info=True)
        return {}

    # URL Handlers

    async def refresh_token_post_handler(self, request):
        try:
            body=await request.read()
            body=body.decode()
            post_data=json.loads(body)
            self.log.info('.. refresh token request for %s' % post_data)
            postuser=str(post_data['user']).lower()
            refresh_token=str(post_data['refresh_token']).lower()
            check=await self.auth.get_token_from_refresh(postuser, post_data['refresh_token'])
        except:
            self.log.error('!! error with login post', exc_info=True)
            check=False
        if check:
            return self.json_response({'access_token': check})

        raise web.HTTPUnauthorized()


    async def login_post_handler(self, request):
        
        try:
            body=await request.read()
            body=body.decode()
            #post_data = await request.post()
            post_data=json.loads(body)
            self.log.info('.. login request for %s' % post_data['user'])
            postuser=str(post_data['user']).lower()
            tokens=await self.auth.get_token_from_credentials(postuser, post_data['password'])
            if 'refresh_token' in tokens and 'access_token' in tokens:
                tokens['user']=postuser
                return self.json_response(tokens)
        except:
            self.log.error('!! error with login post', exc_info=True)

        raise web.HTTPUnauthorized()

        
    async def logout_handler(self, request):
        return self.json_response({"loggedIn":False})  

        
    async def root_handler(self, request):
        try:
            return web.FileResponse(os.path.join(self.config.client_build_directory,'index.html'))
        except:
            return aiohttp.web.HTTPFound('/login')

    async def video_handler(self, request):
        try:
            adaptername=request.match_info['camera'].split('/')[0]
            cameraname=request.match_info['camera'].split('/')[1]
            endpointId="%s:camera:%s" % (adaptername, cameraname)
            if (not endpointId in self.video_keepalives) or ((datetime.datetime.now()-self.video_keepalives[endpointId]).total_seconds() > 20):
                    
                keepalive_directive={   "directive": {
                                            "header": {
                                                "namespace": "Sofa.CameraStreamController", 
                                                "name": "KeepAlive", 
                                                "messageId": str(uuid.uuid1()),
                                                "correlationToken": str(uuid.uuid1()),
                                                "payloadVersion": "3"
                                            },
                                            "endpoint": {
                                                "scope": {
                                                    "type": "BearerToken",
                                                    "token": "fake_temporary",
                                                },
                                                "endpointId": endpointId
                                            },
                                            "payload": {}
                                    }}
                                    
                await self.dataset.sendDirectiveToAdapter(keepalive_directive)
                self.video_keepalives[endpointId]=datetime.datetime.now()
                
            if not os.path.isfile(os.path.join(self.config.video_directory, request.match_info['camera'])):
                # This will keep the player trying while a stalled camera restarts the stream
                blank_m3u="#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:5\n#EXT-X-MEDIA-SEQUENCE:1\n"
                return aiohttp.web.Response(content_type="text/html", body=blank_m3u)
            return aiohttp.web.FileResponse(os.path.join(self.config.video_directory, request.match_info['camera']))
        except:
            self.log.error('!! error in video handler', exc_info=True)
            return self.json_response({})
            

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
        

    async def directiveHandler(self, request):
        
        # Take alexa directive commands such as 'TurnOn' or 'SelectInput'
        response={}
        
        try:
            if request.body_exists:
                body=await request.read()
                data=json.loads(body.decode())
                if 'directive' in data:
                    self.log.info("<- %s %s %s/%s" % (self.get_ip(request), data['directive']['header']['name'], data['directive']['endpoint']['endpointId'], data['directive']['header']['namespace'].split('.')[1]))

                    #self.log.info('<- %s %s: %s' % (self.get_ip(request), data['directive']['header']['name'], data))
                    response=await self.dataset.sendDirectiveToAdapter(data)
                    return self.json_response(response)
        except:
            self.log.error('Error transferring directive: %s' % body,exc_info=True)
        return self.json_response({})
        

    async def listHandler(self, request):

        try:
            result={}
            item="%s?%s" % (request.match_info['list'], request.query_string)
            item=request.match_info['list']
            source=item.split('/',1)[0] 
            headers = { 'authorization': self.dataset.token }
            url = '%s/list/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
            timeout = aiohttp.ClientTimeout(total=self.adapterTimeout)
            async with aiohttp.ClientSession(timeout=timeout) as client:
                async with client.get(url, headers=headers) as response:
                    result=await response.read()
                    result=json.loads(result.decode())

            return self.json_response(result)

        except aiohttp.client_exceptions.ClientConnectorError:
            self.log.error("!! list handler - event gateway connection client connector error")
        except ConnectionRefusedError:
            self.log.error("!! list handler - event gateway connection refused")
        except concurrent.futures._base.TimeoutError:
            self.log.error('!! list handler - error getting list data %s (timed out)' % item)
        except concurrent.futures._base.CancelledError:
            self.log.error('!! list handler - error getting list data %s (cancelled)' % item)
        except:
            self.log.error('!! list handler - error getting list data %s , %s' % ( item, result), exc_info=True)
        
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
                result='#'
                url = '%s/list/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
                self.log.info('>> Posting list request to %s %s' % (source, item.split('/',1)[1] ))
                async with aiohttp.ClientSession() as client:
                    async with client.post(url, data=body) as response:
                        result=await response.read()
                        result=result.decode()
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)
        
        return self.json_response(result)


    async def imageGetter(self, item, width=640, thumbnail=False):

        try:
            if not self.dataset.token:
                return None

            reqstart=datetime.datetime.now()
            source=item.split('/',1)[0] 
            result='#'
            if thumbnail:
                #url = '%s/thumbnail/%s' % (self.dataset.adapters[source]['url'], item.split('/',1)[1] )
                url = '%s/thumbnail/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
            else:
                #url = '%s/image/%s' % (self.dataset.adapters[source]['url'], item.split('/',1)[1] )
                url = '%s/image/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
            timeout = aiohttp.ClientTimeout(total=self.image_timeout)
            headers = { 'authorization': self.dataset.token }
            async with aiohttp.ClientSession(connector=self.gateway_conn, connector_owner=False, timeout=timeout) as client:
                async with client.get(url, headers=headers) as response:
                    result=await response.read()
                    return result
                    #result=result.decode()
                    if str(result)[:10]=="data:image":
                        #result=base64.b64decode(result[23:])
                        self.imageCache[item]=str(result)
                        return result
            return None
        except aiohttp.client_exceptions.ClientConnectorError:
            self.log.error('!! error getting image %s (client connect error)' % item)
        except concurrent.futures._base.CancelledError:
            self.log.warning('.. image request cancelled after %s seconds for %s' % ((datetime.datetime.now()-reqstart).total_seconds(), item))
        except concurrent.futures._base.TimeoutError:
            self.log.warning('.. image request timeout after %s seconds for %s' % ((datetime.datetime.now()-reqstart).total_seconds(), item))
        except:
            self.log.error('Error after %s seconds getting image %s' % ((datetime.datetime.now()-reqstart).total_seconds(), item), exc_info=True)
            return None


    async def imageHandler(self, request):

        try:
            fullitem="%s?%s" % (request.match_info['item'], request.query_string)
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
                    source=item.split('/',1)[0] 
                    headers = { 'authorization': self.dataset.token }
                    url = '%s/save/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body, headers=headers) as response:
                            result=await response.read()
                            result=result.decode()
                            self.log.info('resp: %s' % result)
                except:
                    self.log.error('Error transferring command: %s' % body,exc_info=True)
    
                return aiohttp.web.Response(text=result)
        except:
            self.log.error('Error with save handler', exc_info=True)
        return aiohttp.web.Response(text="")

    async def adapterDelHandler(self, request):
            
        result=""
        try:
            outputData={}   
            body=""
            if request.body_exists:
                body=await request.read()
            #item="%s?%s" % (request.match_info['del'], request.query_string)
            item=request.match_info['del']
            source=item.split('/',1)[0] 
            headers = { 'authorization': self.dataset.token }
            url = '%s/del/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
            self.log.info('Posting Delete Data to: %s' % url)
            async with aiohttp.ClientSession() as client:
                async with client.post(url, data=body, headers=headers) as response:
                    result=await response.read()
                    result=result.decode()
                    self.log.info('resp: %s' % result)
            
        except:
            self.log.error('Error transferring del command: %s' % body,exc_info=True)

        return aiohttp.web.Response(text=result)

    async def adapterAddHandler(self, request):
            
        result=""
        try:
            outputData={}   
            body=""
            if request.body_exists:
                body=await request.read()
            #item="%s?%s" % (request.match_info['del'], request.query_string)
            item=request.match_info['add']
            source=item.split('/',1)[0] 
            headers = { 'authorization': self.dataset.token }
            url = '%s/add/%s/%s' % (self.config.api_gateway, source, item.split('/',1)[1] )
            self.log.info('Posting Add Data to: %s' % url)
            async with aiohttp.ClientSession() as client:
                async with client.post(url, data=body, headers=headers) as response:
                    result=await response.read()
                    result=result.decode()
                    self.log.info('resp: %s' % result)
            
        except:
            self.log.error('Error transferringadd command: %s' % body,exc_info=True)

        return aiohttp.web.Response(text=result)


    def alexa_json_filter(self, data, namespace="", level=0):
        
        try:
            out_data={}
            if 'directive' in data:
                out_data['type']='Directive'
                out_data['name']=data['directive']['header']['name']
                out_data['namespace']=data['directive']['header']['namespace'].split('.')[1]
                out_data['endpointId']=data['directive']['endpoint']['endpointId']
                out_text="%s: %s/%s %s" % (out_data['type'], out_data['namespace'], out_data['name'], out_data['endpointId'])
                
            elif 'event' in data:
                # CHEESE: Alexa API formatting is weird with the placement of payload
                out_data['type']='Event'
                out_data['name']=data['event']['header']['name']
                if data['event']['header']['name']=='ErrorResponse':
                    return "%s: %s %s" % (out_data['name'], out_data['endpointId'], data['event']['payload'])    
                
                if data['event']['header']['namespace'].endswith('Discovery'):
                    if 'payload' in data['event'] and 'endpoints' in data['event']['payload']:
                        out_data['endpointId']='['
                        for item in data['event']['payload']['endpoints']:
                            out_data['endpointId']+=item['endpointId']+" "
                    out_text="%s: %s" % (out_data['name'], out_data['endpointId'])    
                    return out_text

                out_data['endpointId']=data['event']['endpoint']['endpointId']
                out_text="%s: %s" % (out_data['name'], out_data['endpointId'])

                if 'payload' in data['event'] and data['event']['payload']:
                    out_text+=" %s" % data['event']['payload']
                elif 'payload' in data and data['payload']:
                    out_text+=" %s" % data['payload']
                    
                if namespace:
                    out_text+=" %s:" % namespace
                    for prop in data['context']['properties']:
                        if prop['namespace'].endswith(namespace):
                            out_text+=" %s : %s" % (prop['name'], prop['value'])

            else:
                self.log.info('.. unknown response to filter: %s' % data)
                return data

            return out_text
        except:
            self.log.error('Error parsing alexa json', exc_info=True)
            return data


    def get_ip(self, request):
        try:
            return request.headers['X-Real-IP']
        except:
            return request.remote

    async def register_device_handler(self, request):
          
        result={} 
        if request.body_exists:
            try:
                result={}
                outputData={}
                body=await request.read()
                body=body.decode()
                data=json.loads(body)
                if 'remove' in data and len(data['remove'])>0:
                    #self.log.info('>> unregister devices for %s/%s: %s' % (request.user.login, request.session, data['remove']))
                    for item in data['remove']:
                        if item in self.session_device_filter[request.session]:    
                            self.session_device_filter[request.session].remove(item)
                
                if 'add' in data and len(data['add'])>0:
                    #self.log.info('>> register devices for %s/%s: %s' % (request.user.login, request.session, data['add']))
                    if request.session in self.session_device_filter:
                        for item in data['add']:
                            if item not in self.session_device_filter[request.session]:
                                self.session_device_filter[request.session].append(item)
                    else:
                        self.session_device_filter[request.session]=data['add']
                
                # TODO/CHEESE: Pulling data is a multi-part async operation that does not handle a single return well
                # Requiring SSE as the dump until we sort out how to make it better
                
                if request.session in self.active_sse:
                    result = await self.sseDataUpdater(self.active_sse[request.session], devicelist=data['add'])
                else:
                    self.log.warning('!! no current SSE for %s' % request.session)
                #item="%s?%s" % (request.match_info['list'], request.query_string)
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)
        
        return self.json_response(result)


    @login_required
    async def sse_handler(self, request):
        try:
            remoteuser=request.user
            sessionid=request.session
            #self.session_device_filter[sessionid]=[]
            if self.get_ip(request) not in self.active_sessions:
                self.active_sessions[sessionid]=self.get_ip(request)

            self.log.info('++ SSE started for %s/%s' % (self.get_ip(request), request.user))
            
            client_sse_date=datetime.datetime.now(datetime.timezone.utc)
            async with sse_response(request) as resp:
                self.active_sse[sessionid]=resp
                await self.sseDeviceUpdater(resp, self.get_ip(request))
                #if sessionid not in self.session_device_filter:
                #    self.log.info('!! WARNING - session not in filter: %s' % sessionid)
                #    await self.sseDataUpdater(resp)
                #self.log.info('.. initial SSE data load complete')

                while self.adapter.running:
                    if self.sse_last_update>client_sse_date:
                        if request.collector:
                            sendupdates=[]
                            for update in reversed(self.sse_updates):
                                if update['date']>client_sse_date:
                                    filtered=await self.filterUpdate(update['message'], sessionid)
                                    if filtered:
                                        sendupdates.append(filtered)
                                else:
                                    break
                            for update in reversed(sendupdates):
                                #self.log.info('Sending SSE update: %s' % update )
                                await resp.send(json.dumps(update))
                        client_sse_date=self.sse_last_update
                        
                    if client_sse_date<datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10):
                        data={"event": {"header": {"name": "Heartbeat"}}, "heartbeat":self.sse_last_update, "lastupdate":self.sse_last_update }
                        await resp.send(json.dumps(data,default=self.date_handler))
                        client_sse_date=datetime.datetime.now(datetime.timezone.utc)
                    await asyncio.sleep(.1)

                del self.active_sessions[sessionid]
                del self.active_sse[sessionid]
            return resp
        except concurrent.futures._base.CancelledError:
            self.log.info('-- SSE closed for %s/%s' % (self.get_ip(request), remoteuser))
            del self.active_sessions[sessionid]
            return resp
        except:
            self.log.error('!! Error in SSE loop for %s/%s' % (self.get_ip(request), remoteuser), exc_info=True)
            del self.active_sessions[sessionid]
            return resp


    async def filterUpdate(self, message, session_id):
        try:
            if session_id not in self.session_device_filter:
                return message
            if message['event']['header']['name']=='ChangeReport':
                endpointId=message['event']['endpoint']['endpointId']
                if endpointId in self.session_device_filter[session_id]:
                    #self.log.info('+> endpoint: %s / %s' % (message['event']['endpoint']['endpointId'],self.session_device_filter[session_id] ))
                    return message
                else:
                    #self.log.info('-> filtered: %s / %s' % (message['event']['endpoint']['endpointId'], self.session_device_filter[session_id] ))
                    return None

            if message['event']['header']['name']=='Multistate':

                self.log.info('ms: %s %s' % (len(message['state'].keys()), message['state'].keys()))
                new_message_state={}
                for endpointId in message['state']:
                    if endpointId in self.session_device_filter[session_id]:
                        new_message_state[endpointId]=message['state'][endpointId]
                message['state']=new_message_state
                self.log.info('fms: %s %s' % (len(message['state'].keys()), message['state'].keys()))
                if len(message['state'].keys())>0:
                    return message
                else:
                    #self.log.info('-> filtered: %s / %s' % (message['event']['endpoint']['endpointId'], self.session_device_filter[session_id] ))
                    return None
                    
            return message
            
        except:
            self.log.error('!! Error in SSE filter update %s/%s' % (message, session_id), exc_info=True)
        

    async def sseDeviceUpdater(self, resp, remote_ip, devicelist=None):
        try:
            outlist=[]
            byadapter={}
            
            # Carving this up by adapter to deal with the 128k size limitation issues with aiohttp_sse
            # Data updates were already by adapter due to the way we request information
            # https://github.com/rtfol/aiohttp-sse-client/issues/11
            
            if not devicelist:
                devicelist=self.dataset.devices
            
            for dev in devicelist:
                dei=self.dataset.devices[dev]['endpointId'].split(':')[0]
                if dei not in byadapter:
                    byadapter[dei]=[]
                byadapter[dei].append(self.dataset.devices[dev])
            for adapter in byadapter:
                aou={"event": { "header": { "namespace": "Alexa.Discovery", "name": "AddOrUpdateReport", "payloadVersion": "3", "messageId": str(uuid.uuid1()) }, "payload": {"endpoints": byadapter[adapter]}}}
                await resp.send(json.dumps(aou, default=self.date_handler))
            self.log.info('-> %s devicelist' % remote_ip)
        except:
            self.log.error('!! SSE Error transferring list of devices',exc_info=True)


    async def sseDataUpdater(self, resp, devicelist=None):
        try:
            if resp==None:
                self.log.error('!! ERROR - No resp provided for response: %s.' % resp)
                return False 
            req_start=datetime.datetime.now()
            devoutput={}
            #devices=list(self.dataset.devices.values())
            if not devicelist:
                devicelist=self.dataset.devices
            
            getByAdapter={} 
            for dev in devicelist:
                adapter=dev.split(':')[0]
                if adapter not in getByAdapter:
                    getByAdapter[adapter]=[]
                getByAdapter[adapter].append(dev)
                
            gfa=[]
            
            for adapter in getByAdapter:
                #self.log.info('.. getting states for %s' % adapter)
                gfa.append(self.dataset.requestReportStates(getByAdapter[adapter]))
                
            for f in asyncio.as_completed(gfa):
                devstate = await f  # Await for next result.
                devoutput={"event": { "header": { "name": "Multistate" }}, "state": devstate}
                if resp!=None:
                    await resp.send(json.dumps(devoutput))
                else:
                    self.log.info('Adding SSE Update')
                    self.add_sse_update(devoutput)
                if (datetime.datetime.now()-req_start).total_seconds()>2:
                    # seems to typically take just over .5 seconds
                    self.log.info('.. completed req in %s for %s' % (datetime.datetime.now()-req_start, devstate.keys()))

        except concurrent.futures._base.CancelledError:
            self.log.warn('.. sse update cancelled. %s' % devoutput)
                    
        except:
            self.log.error('Error sse list of devices', exc_info=True)


    def add_sse_update(self, message):
        try:
            
            #self.log.info('add sse: %s' % message)
            clearindex=0
            for i,update in enumerate(self.sse_updates):
                if update['date']<datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120):
                    clearindex=i+1
                    break
            #self.log.info('updates that have aged out: %s' % clearindex)
            if clearindex>0:
                del self.sse_updates[:clearindex]
            self.sse_updates.append({'date': datetime.datetime.now(datetime.timezone.utc), 'message':message})
            self.sse_last_update=datetime.datetime.now(datetime.timezone.utc)
        except:
            self.log.error('Error adding update to SSE', exc_info=True)
            
    async def device_list_handler(self, request):
        try:
            return self.json_response(self.dataset.devices)
        except:
            self.log.error('!! error with device list handler', exc_info=True)
        return self.json_response([])



class ui(sofabase):
    
    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.token_secret=self.set_or_default('token_secret', mandatory=True)
            self.web_address=self.set_or_default('web_address', mandatory=True)
            self.web_port=self.set_or_default('web_port', 443)
            self.token_expires=self.set_or_default('token_expires', default=604800)
            self.client_build_directory=self.set_or_default('client_build_directory', mandatory=True)
            self.web_certificate=self.set_or_default('web_certificate', mandatory=True)
            self.web_certificate_key=self.set_or_default('web_certificate_key', mandatory=True) 
            
            
    class adapterProcess(SofaCollector.collectorAdapter):

        async def start(self):
            self.log.info('.. Starting ui server')
            self.uiServer = sofaWebUI(config=self.config, loop=self.loop, log=self.log, dataset=self.dataset, adapter=self)
            await self.uiServer.initialize()


        async def handleStateReport(self, message):
            try:
                await super().handleStateReport(message)
                self.uiServer.add_sse_update(message)
            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)


        async def handleAddOrUpdateReport(self, message):
            try:
                await super().handleAddOrUpdateReport(message)
                if message and self.uiServer:
                    try:
                        if self.config.log_changes:
                            self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        self.uiServer.add_sse_update(message)
                    except:
                        self.log.warn('!. bad or empty AddOrUpdateReport message not sent to SSE: %s' % message, exc_info=True)
            except:
                self.log.error('Error updating from change report', exc_info=True)


        async def handleChangeReport(self, message):
            try:
                #self.log.info('HCR')
                await super().handleChangeReport(message)
                if message:
                    try:
                        if self.config.log_changes:
                            self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        self.uiServer.add_sse_update(message)
                    except:
                        self.log.warn('!. bad or empty ChangeReport message not sent to SSE: %s' % message, exc_info=True)
            except:
                self.log.error('Error updating from change report', exc_info=True)


        async def handleDeleteReport(self, message):
            try:
                self.log.info('!! delete report: %s' % message)
                await super().handleDeleteReport(message)
                self.uiServer.add_sse_update(message)
            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)

        async def virtualList(self, itempath, query={}):
            try:
                if itempath=="activations":
                    return self.uiServer.pending_activations
            except:
                self.log.error('Error getting virtual list for %s' % itempath, exc_info=True)
            return {}



if __name__ == '__main__':
    adapter=ui(name='ui')
    adapter.start()