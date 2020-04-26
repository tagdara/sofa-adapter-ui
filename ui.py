#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase
from sofabase import adapterbase
from sofacollector import SofaCollector
import devices

import subprocess
import math
import random
import json
import asyncio
import aiohttp
from aiohttp import web

from aiohttp_sse import sse_response
import aiohttp_cors

#import aiohttp_jinja2
#import jinja2

import concurrent.futures
import aiofiles
import datetime
import re
import dpath
import urllib
import base64
import ssl
#import inspect

import jwt
#from aiohttp_jwt import JWTMiddleware, login_required

import uuid

class User:

    def __init__(self, id, email, password, is_admin):
        self.id = id
        self.email = email
        self.password = password
        self.is_admin = is_admin

    def __repr__(self):
        template = 'User id={s.id}: <{s.email}, is_admin={s.is_admin}>'
        return template.format(s=self)

    def __str__(self):
        return self.__repr__()

    def match_password(self, password):
        if password != self.password:
            raise User.PasswordDoesNotMatch

    class DoesNotExist(BaseException):
        pass

    class TooManyObjects(BaseException):
        pass

    class PasswordDoesNotMatch(BaseException):
        pass

    class objects:
        _storage = []
        _max_id = 0

        @classmethod
        def create(cls, email, password, is_admin=False):
            cls._max_id += 1
            cls._storage.append(User(cls._max_id, email, password, is_admin))

        @classmethod
        def all(cls):
            return cls._storage

        @classmethod
        def filter(cls, **kwargs):
            users = cls._storage
            for k, v in kwargs.items():
                if v:
                    users = [u for u in users if getattr(u, k, None) == v]
            return users

        @classmethod
        def get(cls, id=None, email=None):
            users = cls.filter(id=id, email=email)
            if len(users) > 1:
                raise User.TooManyObjects
            if len(users) == 0:
                raise User.DoesNotExist
            return users[0]
            
class sofaWebUI():
    
    def __init__(self, config=None, users=None, loop=None, log=None, request=None, dataset=None, notify=None, discover=None, adapter=None):
        self.config=config
        self.users=users
        self.log=log
        self.loop = loop
        self.request=request
        self.workloadData={}
        self.adapter=adapter
        self.filecache={}
        self.cardcache={}
        self.fieldcache={}
        self.dataset=dataset
        self.allowCardCaching=False
        self.notify=notify
        self.discover=discover
        self.imageCache={}
        self.stateReportCache={}
        self.layout={}
        self.adapterTimeout=2
        self.sse_updates=[]
        self.sse_last_update=datetime.datetime.now(datetime.timezone.utc)
        self.active_sessions={}
        #self.sharable_secret = 'secret'
        self.middleware=None
        #self.middleware=JWTMiddleware(
        #    secret_or_pub_key=self.sharable_secret, request_property='user', credentials_required=False)
        # change credentials_required to true to start enforcement
        # todo - build the login process

        self.JWT_SECRET = self.dataset.config['secret']
        self.JWT_ALGORITHM = 'HS256'
        self.JWT_EXP_DELTA_SECONDS = 604800

    async def initialize(self):

        try:
            for user in self.users:
                User.objects.create(email=self.users[user]['email'], password=self.users[user]['password'], is_admin=self.users[user]['admin'])
            
            self.serverAddress=self.config['web_address']
            self.serverApp = web.Application(middlewares=[self.auth_middleware])
            self.cors = aiohttp_cors.setup(self.serverApp, defaults={
                "*": aiohttp_cors.ResourceOptions(allow_credentials=True, expose_headers="*", allow_methods='*', allow_headers="*")
            })
            
            #Access-Control-Allow-Origin
            

            self.cors.add(self.serverApp.router.add_get('/', self.root_handler))

            self.cors.add(self.serverApp.router.add_get('/logout', self.logout_handler))
            self.cors.add(self.serverApp.router.add_post('/login', self.login_post))
            self.cors.add(self.serverApp.router.add_get('/get-user', self.get_user))
            self.cors.add(self.serverApp.router.add_post('/save-user', self.save_user))
            self.serverApp.router.add_get('/loginstatus', self.login_status_handler)
            self.serverApp.router.add_get('/index.html', self.root_handler)
            self.serverApp.router.add_get('/status', self.status_handler)
            
            #self.cors.add(self.serverApp.router.add_route('*', '/directives', self.directivesHandler))
            self.cors.add(self.serverApp.router.add_get('/directives', self.directivesHandler))
            self.cors.add(self.serverApp.router.add_get('/properties', self.propertiesHandler))
            self.cors.add(self.serverApp.router.add_get('/events', self.eventsHandler))
            self.cors.add(self.serverApp.router.add_get('/layout', self.layoutHandler))

            self.serverApp.router.add_get('/data/{item:.+}', self.dataHandler)
            self.cors.add(self.serverApp.router.add_get('/list/{list:.+}', self.listHandler))
            self.serverApp.router.add_get('/var/{list:.+}', self.varHandler)
            self.cors.add(self.serverApp.router.add_post('/list/{list:.+}', self.listPostHandler))
            
            self.serverApp.router.add_get('/adapters', self.adapterHandler)   
            self.serverApp.router.add_get('/restartadapter/{adapter:.+}', self.adapterRestartHandler)
            self.serverApp.router.add_get('/deviceList', self.deviceListHandler)
            self.serverApp.router.add_get('/deviceListWithData', self.deviceListWithDataHandler) # deprecated

            self.serverApp.router.add_post('/deviceState', self.deviceStatePostHandler)
            self.cors.add(self.serverApp.router.add_post('/directive', self.directiveHandler))
            
            self.cors.add(self.serverApp.router.add_post('/add/{add:.+}', self.adapterAddHandler))
            self.cors.add(self.serverApp.router.add_post('/del/{del:.+}', self.adapterDelHandler))  
            self.cors.add(self.serverApp.router.add_post('/save/{save:.+}', self.adapterSaveHandler))
            
            self.serverApp.router.add_get('/displayCategory/{category:.+}', self.displayCategoryHandler)
            self.cors.add(self.serverApp.router.add_get('/image/{item:.+}', self.imageHandler))
            self.cors.add(self.serverApp.router.add_get('/thumbnail/{item:.+}', self.imageHandler))
            self.serverApp.router.add_get('/refresh', self.refresh_handler)  
            self.serverApp.router.add_post('/data/{item:.+}', self.dataPostHandler)
            
            self.cors.add(self.serverApp.router.add_get('/sse', self.sse_handler))
            self.serverApp.router.add_get('/lastupdate', self.sse_last_update_handler)
            
            self.serverApp.router.add_static('/log/', path=self.dataset.baseConfig['logDirectory'])
            if os.path.isdir(self.config['client_build_directory']):
                self.cors.add(self.serverApp.router.add_static('/client', path=self.config['client_build_directory'], append_version=True))
                self.cors.add(self.serverApp.router.add_static('/fonts', path=self.config['client_build_directory']+"/fonts", append_version=True))
            else:
                self.log.error('!! Client build directory does not exist.  Cannot host client until this directory is created and this adapter is restarted')

            self.runner=aiohttp.web.AppRunner(self.serverApp)
            await self.runner.setup()

            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self.ssl_context.load_cert_chain(self.config['certificate'], self.config['certificate_key'])

            self.site = web.TCPSite(self.runner, self.config['web_address'], self.config['port'], ssl_context=self.ssl_context)
            await self.site.start()

        except:
            self.log.error('Error with ui server', exc_info=True)
            
    # Start - This is the JWT testing code
    async def auth_middleware(self, app, handler):
        async def middleware(request):
            try:
                whitelist=['/','/client','/favicon.ico','/login']
                if not request.method=='OPTIONS' and not str(request.rel_url) in whitelist and not str(request.rel_url).startswith('/client') and not str(request.rel_url).startswith('/thumbnail'):
                    request.user = None
                    try:
                        jwt_token = request.headers.get('authorization', None)
                    except:
                        self.log.error('.! could not get jwt token from authorization header', exc_info=True)
                    if not jwt_token:
                        try:
                            if 'token' in request.cookies:
                                #self.log.info('.. token from cookie: %s' % request.cookies['token'])
                                jwt_token=request.cookies['token']
                        except:
                            self.log.error('.! could not get jwt token from cookies', exc_info=True)

                    if not jwt_token:                        
                        # CHEESE: There is probably a better way to get this information, but this is a shim for EventSource not being able
                        # to send an Authorization header from the client side.  It also does not appear send cookies in the normal way
                        # but you can farm them out of the headers
                        try:
                            if 'Cookie' in request.headers:
                                cookies=request.headers['Cookie'].split('; ')
                                for hcookie in cookies:
                                    if hcookie.split('=')[0]=='token':
                                        jwt_token=hcookie.split('=')[1]
                        except:
                            self.log.error('Could not decipher token from header cookies', exc_info=True)

                    if jwt_token:
                        try:
                            payload = jwt.decode(jwt_token, self.JWT_SECRET,
                                                 algorithms=[self.JWT_ALGORITHM])
                        except (jwt.DecodeError, jwt.ExpiredSignatureError):
                            self.log.warn('.- Token is invalid for user. Path: %s' % request.rel_url)
                            return self.json_response({'message': 'Token is invalid'}, status=400)
                        request.user = User.objects.get(id=payload['user_id'])
                    else:
                        self.log.warn('.- No token available for user. Path: %s' % request.rel_url)
                        self.log.warn('-- Headers: %s' % request.headers)
                        return self.json_response({'message': 'Token is missing'}, status=400)

                return await handler(request)
            except aiohttp.web_exceptions.HTTPNotFound:
                # 404's
                pass
            except:
                self.log.error('!! error with jwt middleware handler: %s %s' % ( request.method, request.rel_url), exc_info=True)

        return middleware
    
    def login_required(func):
        def wrapper(self, request):
            if not request.user:
                return self.json_response({'message': 'Auth required'}, status=401)
            return func(self, request)
        return wrapper

    def json_response(self, body='', **kwargs):
        try:
            kwargs['body'] = json.dumps(body or kwargs['body']).encode('utf-8')
            kwargs['content_type'] = 'application/json'
            return web.Response(**kwargs)
        except:
            self.log.error('!! error with json response', exc_info=True)
            return web.Response({'body':''})

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

    
    async def login_post(self, request):
        try:
            post_data = await request.post()
            self.log.info('Login request for %s' % post_data)
            try:
                postuser=str(post_data['user']).lower()
                
                user = User.objects.get(email=postuser)
                user.match_password(post_data['password'])
            except User.DoesNotExist:
                self.log.info('.. incorrect user: %s' % postuser)
                return self.json_response({'message': 'User does not exist'}, status=400)
            except User.PasswordDoesNotMatch:
                self.log.info('.. incorrect password: %s' % post_data['password'])
                return self.json_response({'message': 'Incorrect password'}, status=400)
            
            self.log.info('generating token with %s' % user.id)
            payload = {
                'user_id': user.id,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.JWT_EXP_DELTA_SECONDS)
            }
            jwt_token = jwt.encode(payload, self.JWT_SECRET, self.JWT_ALGORITHM)
            self.log.info('generated token %s' % jwt_token)
            return self.json_response({'token': jwt_token.decode('utf-8')})
        except:
            self.log.error('!! error with login post', exc_info=True)
            return self.json_response({'token': ""})

    # End - This is the JWT testing code       
    
    async def loadData(self, jsonfilename):
        
        try:
            with open(os.path.join(self.dataset.baseConfig['baseDirectory'], 'data', '%s.json' % jsonfilename),'r') as jsonfile:
                return json.loads(jsonfile.read())
        except:
            self.log.error('Error loading pattern: %s' % jsonfilename,exc_info=True)
            return {}


    async def layoutUpdate(self):
        try:
            async with aiofiles.open(os.path.join(self.config['layout_directory'], 'layout.json'), mode='r') as f:
                layout = await f.read()
                return layout
        except:
            self.log.error('Error getting file for cache: %s' % filename, exc_info=True)

    async def login_status_handler(self, request):
        try:
            await check_permission(request, 'main')
            self.log.info('status: Logged In.')
            return web.Response(text=json.dumps({'loggedIn':True}))
            
        except:
            self.log.error('Error in login status process', exc_info=True)
            self.log.info('status: not Logged In.')
            return web.Response(text=json.dumps({'loggedIn':False}))

    async def login_handler(self, request):
        try:
            self.log.info('Logging in as sofa')
            redirect_response = web.HTTPFound('/')
            await remember(request, redirect_response, 'sofa')
            return aiohttp.web.HTTPFound('/')
            
        except:
            self.log.error('Error in login process', exc_info=True)
            return aiohttp.web.HTTPFound('/login')


    async def login_post_handler(self, request):

        try:
            redirect_response = web.HTTPFound('/login')
            await forget(request, redirect_response)
            if request.body_exists:
                body=await request.read()
                data=json.loads(body.decode())
                self.log.info('Login request for %s' % data)
                if data['user']=='sofa':
                    self.log.info('Logged In.')
                    redirect_response = web.HTTPFound('/')
                    await remember(request, redirect_response, 'sofa')
                    return web.Response(text=json.dumps({"loggedIn":True}))
                else:
                    self.log.info('Not Logged In.')
                    await forget(request, redirect_response)
                    return web.Response(text=json.dumps({"loggedIn":False}))    
            else:
                self.log.info('Not Logged In.')
                return web.Response(text=json.dumps({"loggedIn":False}))    

        except:
            self.log.error('Error handling login attempt' ,exc_info=True)
            return web.Response(text=json.dumps({"loggedIn":False}))    

            
    async def logout_handler(self, request):
        try:
            redirect_response = web.HTTPFound('/')
            #await forget(request, redirect_response)
            raise redirect_response
        except:
            self.log.error('Error in login process', exc_info=True)
            return web.Response(text=json.dumps({"loggedIn":False}))    

    async def api_check_handler(self, request):
        try:
            await check_permission(request, 'api')
            return web.Response(body="I can access the api")
        except:
            self.log.error('Error in login process', exc_info=True)

    async def layoutHandler(self, request):
        #if not self.layout:
        #    self.layout=
        self.layout=await self.layoutUpdate()
            
        return web.Response(content_type="text/html", body=json.dumps(json.loads(self.layout)))

    async def directivesHandler(self, request):
        try:
            #await check_permission(request, 'api')
            directives=await self.dataset.getAllDirectives()
            return web.Response(text=json.dumps(directives))
        except:
            self.log.error('Error with Directives Handler', exc_info=True)
            return web.Response(text=json.dumps({'Error':True}))


    async def propertiesHandler(self, request):
        
        properties=await self.dataset.getAllProperties()
        return web.Response(text=json.dumps(properties))
 
 
    async def eventsHandler(self, request):
        
        eventSources={ 'DoorbellEventSource': { "event": "doorbellPress"}}
        return web.Response(text=json.dumps(eventSources))
        

    async def dataHandler(self, request):

        try:
            result=await self.loadData(request.match_info['item'])
            if request.query_string:
                result=await self.queryStringAdjuster(request.query_string, result)
        except:
            self.log.error('Did not load data from file for %s' % item)
            result={}
    
        return web.Response(text=json.dumps(result))

        
    async def displayCategoryHandler(self, request):
        
        try:
            category=request.match_info['category']
            devicelist=[]
            alldevices=self.dataset.getObjectFromPath("/devices")
            for device in alldevices:
                try:
                    if category.upper() in alldevices[device]['displayCategories']:
                        devicelist.append(alldevices[device])
                except:
                    pass

            return web.Response(text=json.dumps(devicelist))

        except:
            self.log.error('Error getting items for display category: %s' % category, exc_info=True)


    async def queryStringAdjuster(self, querystring, lookup):
            

        if querystring.find('stateReport')>-1:
            self.log.info('Getting state report from query string adjuster')
            controllers={}
            try:
                #if lookup['endpointId'] not in self.stateReportCache:
                #self.log.info('not in cache: %s' % lookup['endpointId'] )
                newState=await self.dataset.requestReportState(lookup['endpointId'])
                self.stateReportCache[lookup['endpointId']]=json.loads(newState.decode())
                
                self.log.debug('Lookup: %s' % lookup)
                return self.stateReportCache[lookup['endpointId']]
            except:
                self.log.error('Couldnt build state report for %s: %s' % (querystring, lookup), exc_info=True)

                
        elif querystring.find('keynames')>-1:
            namepairs={}
            for item in lookup:
                try:
                    namepairs[item]=lookup[item]['name']
                except:
                    namepairs[item]=item
            lookup=dict(namepairs)

        elif querystring.find('namekeys')>-1:
            namepairs={}
            for item in lookup:
                try:
                    namepairs[lookup[item]['name']]=item
                except:
                    namepairs[item]=item
            lookup=dict(namepairs)

        elif querystring.find('keys')>-1:
            lookup=list(lookup.keys())

        elif querystring.find('names')>-1:
            namepairs=[]
            for item in lookup:
                try:
                    namepairs.append(lookup[item]['name'])
                except:
                    namepairs.append(item)
            lookup=list(namepairs)
            
        return lookup

            
    def date_handler(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            raise TypeError
 

    async def manifestUpdate(self):
        try:
            async with aiofiles.open(os.path.join(self.config['client_static_directory'], 'sofa.appcache'), mode='r') as f:
                manifest = await f.read()
                                           # v-auto
                manifest=manifest.replace('# version-auto', '# v%s' % datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"))
                manifest=manifest.replace('url-auto', 'https://%s' % self.config['web_address'])

                return manifest
        except:
            self.log.error('Error getting file for cache: %s' % filename, exc_info=True)

    async def manifestHandler(self, request):
        return web.Response(content_type="text/html", body=await self.manifestUpdate())

    async def imageGetter(self, item, width=640, thumbnail=False):

        try:
            source=item.split('/',1)[0] 
            if source in self.dataset.adapters:
                result='#'
                if thumbnail:
                    url = 'http://%s:%s/thumbnail/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                else:
                    url = 'http://%s:%s/image/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                async with aiohttp.ClientSession() as client:
                    async with client.get(url) as response:
                        result=await response.read()
                        return result
                        #result=result.decode()
                        if str(result)[:10]=="data:image":
                            #result=base64.b64decode(result[23:])
                            self.imageCache[item]=str(result)
                            return result
            return None

        except concurrent.futures._base.CancelledError:
            self.log.warn('.. image request cancelled for %s' % item)
        except:
            self.log.error('Error getting image %s' % item, exc_info=True)
            return None


    async def imageHandler(self, request):

        try:
            fullitem="%s?%s" % (request.match_info['item'], request.query_string)
            #self.log.info('.. image request: %s (%s)' % (fullitem, request.query_string))
            if fullitem in self.imageCache:
                self.log.info('.. image from cache')
                result=base64.b64decode(self.imageCache[fullitem][23:])
                return web.Response(body=result, headers = { "Content-type": "image/jpeg" })
            
            if "width=" not in request.query_string or request.path.find('thumbnail')>0:
                result=await self.imageGetter(fullitem, thumbnail=True)
            else:
                result=await self.imageGetter(fullitem)
            
            return web.Response(body=result, headers = { "Content-type": "image/jpeg" })
            
            if str(result)[:10]=="data:image":
                result=base64.b64decode(result[23:])
                #result=base64.b64decode(result[23:])
                return web.Response(body=result, headers = { "Content-type": "image/jpeg" })
            
            self.log.info('Did not get an image to return for %s: %s' % (request.match_info['item'], str(result)[:10]))
            return web.Response(content_type="text/html", body='')
        except:
            self.log.error('Error with image handler', exc_info=True)

    async def listHandler(self, request):

        try:
            #self.log.info('List handler: %s ' % request)
            result={}
            item="%s?%s" % (request.match_info['list'], request.query_string)
            item=request.match_info['list']
            source=item.split('/',1)[0] 
            if source in self.dataset.adapters:
                #result='#'
                url = 'http://%s:%s/list/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                #self.log.info('Requesting list data from: %s' % url)
                timeout = aiohttp.ClientTimeout(total=self.adapterTimeout)
                async with aiohttp.ClientSession(timeout=timeout) as client:
                    async with client.get(url) as response:
                        result=await response.read()
                        result=json.loads(result.decode())
                        #self.log.info('resp: %s' % result)
            else:
                self.log.error('Source not in adapters: %s %s' % (source, self.dataset.adapters))

            return web.Response(text=json.dumps(result, default=self.date_handler))

        except aiohttp.client_exceptions.ClientConnectorError:
            self.log.error('Connection refused for adapter %s.  Adapter is likely stopped' % source)
        except ConnectionRefusedError:
            self.log.error('Connection refused for adapter %s.  Adapter is likely stopped' % source)
        except concurrent.futures._base.TimeoutError:
            self.log.error('Error getting list data %s (timed out)' % item)
        except concurrent.futures._base.CancelledError:
            self.log.error('Error getting list data %s (cancelled)' % item)
        except:
            self.log.error('Error getting list data %s' % item, exc_info=True)
        
        return web.Response(text=json.dumps({}, default=self.date_handler))

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
                    url = 'http://%s:%s/list/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    self.log.info('>> Posting list request to %s %s' % (source, item.split('/',1)[1] ))
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)
        
        return self.json_response(result)

    async def varHandler(self, request):

        try:
            # Same as list but not json
            self.log.info('.. var handler: %s ' % request)
            result={}
            item="%s?%s" % (request.match_info['list'], request.query_string)
            item=request.match_info['list']
            source=item.split('/',1)[0] 
            if source in self.dataset.adapters:
                result='#'
                url = 'http://%s:%s/var/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                async with aiohttp.ClientSession() as client:
                    async with client.get(url) as response:
                        result=await response.json()
                        #result=result.decode()
            else:
                self.log.error('Source not in adapters: %s %s' % (source, self.dataset.adapters))

            return self.json_response(result)
            #return web.Response(text=result)

        except concurrent.futures._base.CancelledError:
            self.log.error('Error getting list data %s (cancelled)' % item)
            return self.json_response({})
        except:
            self.log.error('Error getting list data %s' % item, exc_info=True)
            return self.json_response({})


    async def adapterSaveHandler(self, request):
            
        if request.body_exists:
            try:
                outputData={}
                body=await request.read()
                #item="%s?%s" % (request.match_info['save'], request.query_string)
                item=request.match_info['save']
                source=item.split('/',1)[0] 
                if source in self.dataset.adapters:
                    result='#'
                    url = 'http://%s:%s/save/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    async with aiohttp.ClientSession() as client:
                        async with client.post(url, data=body) as response:
                            result=await response.read()
                            result=result.decode()
                            self.log.info('resp: %s' % result)
                
            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)

            return web.Response(text=result)


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

        return web.Response(text=result)

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

            return web.Response(text=result)


    async def directiveHandler(self, request):
        
        # Take alexa directive commands such as 'TurnOn' or 'SelectInput'
        response={}
        
        try:
            if request.body_exists:
                body=await request.read()
                data=json.loads(body.decode())
                if 'directive' in data:
                    self.log.info("<- %s %s %s/%s" % (request.remote, data['directive']['header']['name'], data['directive']['endpoint']['endpointId'], data['directive']['header']['namespace'].split('.')[1]))

                    #self.log.info('<- %s %s: %s' % (request.remote, data['directive']['header']['name'], data))
                    response=await self.dataset.sendDirectiveToAdapter(data)
                    return web.Response(text=json.dumps(response, default=self.date_handler))
                else:
                    return web.Response(text="{}")                    

        except:
            self.log.error('Error transferring directive: %s' % body,exc_info=True)
            return web.Response(text="{}")

    
    async def devicesHandler(self, request):

        try:
            self.log.info('devices handler: %s ' % request)
            return web.Response(text=json.dumps(self.dataset.devices, default=self.date_handler))
        except:
            self.log.error('Error transferring list of devices: %s' % body,exc_info=True)

    async def deviceListHandler(self, request):

        try:
            self.log.info('<- %s devicelist request' % (request.remote))
            outlist=[]
            for dev in self.dataset.devices:
                outlist.append(self.dataset.devices[dev])
            return web.Response(text=json.dumps(outlist, default=self.date_handler))
        except:
            self.log.error('Error transferring list of devices: %s' % body,exc_info=True)

    async def deviceListWithDataHandler(self, request):

        # This is requested at the beginning of a client session to get the full list of devices as well as 
        # any status data. It's combined here to prevent any rendering delays in the UI.   Even though it's a large amount of data
        # testing shows that the ui loads faster and more consistently than trying to pick the individual data needed
        # In the future, server side rendering of the first load may be able to prevent this, and it is overkill on mobile where 
        # the full dashboard layout is not used.

        try:
            self.log.info('** started device and datalist %s' % request.remote)
            devices=list(self.dataset.devices.values())

            getByAdapter={} 
            for dev in self.dataset.devices:
                adapter=dev.split(':')[0]
                if adapter not in getByAdapter:
                    getByAdapter[adapter]=[]
                getByAdapter[adapter].append(dev)
                
            allstates=await asyncio.gather(*[self.dataset.requestReportStates(adapter, getByAdapter[adapter]) for adapter in getByAdapter ])
            
            states={}
            for statelist in allstates:
                for device in statelist:
                    states[device]=statelist[device]
                    
            return web.Response(text=json.dumps({"event": { "header": { "name": "Multistate"}}, "devices":devices, "state": states}, default=self.date_handler))
        except:
            self.log.error('Error transferring list of devices: %s' % self.dataset.devices.keys(),exc_info=True)


    async def deviceStatePostHandler(self, request):
            
        if request.body_exists:
            rqid=str(uuid.uuid1())
            try:
                outputData={}
                body=await request.read()
                devices=json.loads(body.decode('utf-8'))
                getByAdapter={}
                alldevs=[]
                for dev in devices:
                    result=self.adapter.getDeviceByfriendlyName(dev)
                    adapter=dev.split(':')[0]
                    if adapter not in getByAdapter:
                        getByAdapter[adapter]=[]
                    getByAdapter[adapter].append(dev)
                    alldevs.append(dev)
                try:
                    allstates=await asyncio.gather(*[self.dataset.requestReportStates(adapter, getByAdapter[adapter]) for adapter in getByAdapter ], return_exceptions=True)
                except:
                    self.log.error('Error collecting states from adapters', exc_info=True)
                outd={}
                for statelist in allstates:
                    for device in statelist:
                        outd[device]=statelist[device]

                return web.Response(text=json.dumps(outd, default=self.date_handler))
            except:
                self.log.error('Couldnt build device state report', exc_info=True)

            return web.Response(text=json.dumps({}, default=self.date_handler))
            

    async def dataPostHandler(self, request):
            
        if request.body_exists:
            rqid=str(uuid.uuid1())
            try:
                outputData={}
                body=await request.read()
                devices=json.loads(body.decode('utf-8'))
                for dev in devices:
                    result=await self.dataSender("%s/%s" % (request.match_info['item'], dev))
                    if request.query_string:
                        result=await self.queryStringAdjuster(request.query_string, result)
                    outputData[dev]=result

            except:
                self.log.error('Error transferring command: %s' % body,exc_info=True)

            return web.Response(text=json.dumps(outputData, default=self.date_handler))

    async def refresh_handler(self,request):
    
        try:
            await self.discover('sofa')
            return web.Response(text='Discovery request sent')

        except:
            self.log.error('Error running discovery', exc_info=True)
            return web.Response(text='Discovery request failed')
            
    async def adapterHandler(self,request):
        try:
            for adapter in self.dataset.adapters:
                self.dataset.adapters[adapter]['restart']="/restartadapter/%s" % adapter
            return web.Response(text=json.dumps(self.dataset.adapters, default=self.date_handler))
        except:
            self.log.error('Error listing adapters', exc_info=True)
            return web.Response(text="Error listing adapters")
            
    async def adapterRestartHandler(self, request):
        try:
            adapter=request.match_info['adapter']
            stdoutdata = subprocess.getoutput("/opt/sofa-server/svc %s" % adapter)
            return web.Response(text=stdoutdata)

        except:
            self.log.error('Error restarting adapter', exc_info=True)
            return web.Response(text="Error restarting adapter %s" % adapter)

    async def sse_last_update_handler(self, request):
      
        return web.Response(text=json.dumps({"lastupdate":self.sse_last_update},default=self.date_handler))

    async def status_handler(self, request):
      
        return web.Response(text=json.dumps({"sessions":self.active_sessions},default=self.date_handler))

    @login_required
    async def sse_handler(self, request):
        
        try:
            remoteip=request.remote
            sessionid=str(uuid.uuid1())
            if remoteip not in self.active_sessions:
                self.active_sessions[sessionid]=remoteip

            self.log.info('++ SSE started for %s' % request.remote)
            client_sse_date=datetime.datetime.now(datetime.timezone.utc)
            async with sse_response(request) as resp:
                await self.sseDeviceUpdater(resp, remoteip)
                await self.sseDataUpdater(resp)
                while True:
                    if self.sse_last_update>client_sse_date:
                        sendupdates=[]
                        for update in reversed(self.sse_updates):
                            if update['date']>client_sse_date:
                                sendupdates.append(update['message'])
                            else:
                                break
                        for update in reversed(sendupdates):
                            #self.log.info('Sending SSE update: %s' % update )
                            await resp.send(json.dumps(update))
                        client_sse_date=self.sse_last_update
                    if client_sse_date<datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10):
                        await resp.send(json.dumps({"event": {"header": {"name": "Heartbeat"}}, "heartbeat":self.sse_last_update, "lastupdate":self.sse_last_update },default=self.date_handler))
                        client_sse_date=datetime.datetime.now(datetime.timezone.utc)
                    await asyncio.sleep(.1)
                del self.active_sessions[sessionid]
            return resp
        except concurrent.futures._base.CancelledError:
            self.log.info('-- SSE closed for %s' % remoteip)
            del self.active_sessions[sessionid]
            return resp
        except:
            self.log.error('Error in SSE loop', exc_info=True)
            del self.active_sessions[sessionid]
            return resp


    async def sseDeviceUpdater(self, resp, remoteip):

        try:
            self.log.info('<- %s devicelist request' % (remoteip))
            outlist=[]
            for dev in self.dataset.devices:
                outlist.append(self.dataset.devices[dev])
            aou={"event": { "header": { "namespace": "Alexa.Discovery", "name": "AddOrUpdateReport", "payloadVersion": "3", "messageId": str(uuid.uuid1()) }, "payload": {"endpoints": outlist}}}

            await resp.send(json.dumps(aou, default=self.date_handler))
            #return web.Response(text=json.dumps(outlist, default=self.date_handler))
        except:
            self.log.error('!! SSE Error transferring list of devices',exc_info=True)


    async def sseDataUpdater(self, resp):

        try:
            devoutput={}
            devices=list(self.dataset.devices.values())

            getByAdapter={} 
            for dev in self.dataset.devices:
                adapter=dev.split(':')[0]
                if adapter not in getByAdapter:
                    getByAdapter[adapter]=[]
                getByAdapter[adapter].append(dev)
                
            gfa=[]
            
            for adapter in getByAdapter:
                gfa.append(self.dataset.requestReportStates(adapter, getByAdapter[adapter]))
                
            for f in asyncio.as_completed(gfa):
                devstate = await f  # Await for next result.
                devoutput={"event": { "header": { "name": "Multistate" }}, "state": devstate}
                await resp.send(json.dumps(devoutput))

        except concurrent.futures._base.CancelledError:
            self.log.warn('.. sse update cancelled. %s' % devoutput)
                    
        except:
            self.log.error('Error sse list of devices', exc_info=True)



    async def root_handler(self, request):
        try:
            #return web.FileResponse(os.path.join(self.config['client_static_directory'],'index.html'))
            return web.FileResponse(os.path.join(self.config['client_build_directory'],'index.html'))
        except:
            return aiohttp.web.HTTPFound('/login')

    def add_sse_update(self, message):
        
        try:
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


class ui(sofabase):

    class adapterProcess(SofaCollector.collectorAdapter):

        def __init__(self, log=None, loop=None, dataset=None, notify=None, discover=None, request=None,  **kwargs):
            self.uiServer=None
            self.dataset=dataset
            self.config=self.dataset.config
            self.log=log
            self.notify=notify
            self.request=request
            self.discover=discover
            self.loop=loop
            
            
        async def start(self):
            self.log.info('.. Starting ui server')
            self.users=self.loadJSON("users")
            self.uiServer = sofaWebUI(config=self.config, users=self.users, loop=self.loop, log=self.log, request=self.request, dataset=self.dataset, notify=self.notify, discover=self.discover, adapter=self)
            await self.uiServer.initialize()
            #await self.discover('sofa')

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
                        #if 'log_change_reports' in self.dataset.config:
                        self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        self.uiServer.add_sse_update(message)
                    except:
                        self.log.warn('!. bad or empty AddOrUpdateReport message not sent to SSE: %s' % message, exc_info=True)

            except:
                self.log.error('Error updating from change report', exc_info=True)


        async def handleChangeReport(self, message):
        
            try:
                await super().handleChangeReport(message)
                if message:
                    try:
                        if 'log_change_reports' in self.dataset.config:
                            self.log.info('-> SSE %s %s' % (message['event']['header']['name'],message))
                        self.uiServer.add_sse_update(message)
                    except:
                        self.log.warn('!. bad or empty ChangeReport message not sent to SSE: %s' % message, exc_info=True)

            except:
                self.log.error('Error updating from change report', exc_info=True)

        async def handleDeleteReport(self, message):
        
            try:
                await super().handleDeleteReport(message)
                self.uiServer.add_sse_update(message)

            except:
                self.log.error('Error updating from state report: %s' % message, exc_info=True)

        async def virtualCategory(self, category):
            
            if category in ['light','thermostat']:
                subset={key: value for (key,value) in self.dataset.devices.items() if category.upper() in value['displayCategories']}
            else:
                subset={}
                
            return subset


if __name__ == '__main__':
    adapter=ui(name='ui')
    adapter.start()