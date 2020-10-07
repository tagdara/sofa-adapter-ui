import jwt
from aiohttp import web
import datetime
import uuid

class api_consumer:

    def __init__(self, id, name, api_key, collector=False):
        self.id = id
        self.name = name
        self.api_key = api_key
        self.collector = collector

    def __repr__(self):
        template = 'api_consumer {s.name} <id={s.id}, api_key={s.api_key} collector={s.collector} >'
        return template.format(s=self)

    def __str__(self):
        return self.__repr__()

    def match_key(self, api_key):
        if api_key != self.api_key:
            raise api_consumer.InvalidAPIKey

    class DoesNotExist(BaseException):
        pass

    class TooManyObjects(BaseException):
        pass

    class InvalidAPIKey(BaseException):
        pass

    class objects:
        _storage = []
        _max_id = 0

        @classmethod
        def create(cls, name, api_key):
            cls._max_id += 1
            cls._storage.append(api_consumer(cls._max_id, name, api_key))

        @classmethod
        def delete(cls, name):
            consumers = cls.filter(name=name)
            for consumer in consumers:
                cls._storage.remove(consumer)

        @classmethod
        def all(cls):
            return cls._storage

        @classmethod
        def filter(cls, **kwargs):
            consumers = cls._storage
            for k, v in kwargs.items():
                if v:
                    consumers = [u for u in consumers if getattr(u, k, None) == v]
            return consumers

        @classmethod
        def get(cls, id=None, name=None):
            consumers = cls.filter(id=id, name=name)
            if len(consumers) > 1:
                raise api_consumer.TooManyObjects
            if len(consumers) == 0:
                raise api_consumer.DoesNotExist
            return consumers[0]
            

class Auth():

    def __init__(self, log=None, secret="no_secret", token_expires=604800, algorithm='HS256'): 
        self.JWT_SECRET = secret
        self.JWT_ALGORITHM = algorithm
        self.JWT_EXP_DELTA_SECONDS = token_expires
        # TODO/CHEESE - Thumbnails should have security but some non-authenticated API such
        # as the homekit camera and jukebox may be using it
        self.whitelist=['/eventgateway/activate','/devices','/activate', '/client','/favicon.ico','/login','/logout','/thumbnail','/fonts']
        self.log=log
        self.instance_id=str(uuid.uuid1())

    # Start - This is the JWT testing code
    async def middleware(self, app, handler):

        async def token_check(request):
            
            #if str(request.rel_url)=="/":
            #    return await handler(request)                
            
            for item in self.whitelist:
                if str(request.rel_url).startswith(item):
                    return await handler(request)
            
            if not request.method=='OPTIONS':
                request.api_consumer = None
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
                        self.log.warn('.- Token is invalid. Path: %s' % request.rel_url)
                        raise web.HTTPUnauthorized()
                        #return self.json_response({'message': 'Token is invalid'}, status=400)
                    
                    if 'instance' not in payload or payload['instance']!=self.instance_id:
                        self.log.debug('.- Token not correct for this instance. Path: %s' % request.rel_url)
                        #self.log.warn('-- Headers: %s' % request.headers)
                        raise web.HTTPUnauthorized()
                    
                    try:
                        request.api_consumer = payload['name']
                        if 'collector' in payload:
                            request.collector = payload['collector']
                        else:
                            request.collector = False

                    except:
                        self.log.error('.. error dealing with payload: %s' % payload, exc_info=True)
                else:
                    self.log.warn('.- No token available for api consumer. Path: %s' % request.rel_url)
                    #self.log.warn('-- Headers: %s' % request.headers)
                    raise web.HTTPUnauthorized()
                    #return self.json_response({'message': 'Token is missing'}, status=400)
            
            return await handler(request)
            
        return token_check
        

    async def get_token_from_api_key(self, name, api_key):
        try:
            try:
                consumer = api_consumer.objects.get(name=name)
                consumer.match_key(api_key)
            except api_consumer.DoesNotExist:
                self.log.info('.. consumer not activated for this key: %s' % name)
                return False

            except api_consumer.InvalidAPIKey:
                self.log.info('.. incorrect API key: %s' % api_key)
                return False

            payload = {
                'name': consumer.name,
                'collector': consumer.collector,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.JWT_EXP_DELTA_SECONDS),
                "instance": self.instance_id
            }
            jwt_token = jwt.encode(payload, self.JWT_SECRET, self.JWT_ALGORITHM)
            #self.log.info('generated token %s' % jwt_token)
            return jwt_token.decode('utf-8')
        except:
            self.log.error('!! error with api key check post', exc_info=True)
            return False


