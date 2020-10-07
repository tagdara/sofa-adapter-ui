import jwt
from aiohttp import web
import datetime
import uuid
<<<<<<< HEAD
=======
import os
import json

class User:

    def __init__(self, id, login, email, password, is_admin):
        self.id = id
        self.login = login
        self.email = email
        self.password = password
        self.is_admin = is_admin

    def __repr__(self):
        template = 'User {s.login} <id={s.id}, email={s.email}, is_admin={s.is_admin}>'
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
        def create(cls, login, email, password, is_admin=False):
            cls._max_id += 1
            cls._storage.append(User(cls._max_id, login, email, password, is_admin))

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
        def get(cls, id=None, login=None):
            users = cls.filter(id=id, login=login)
            if len(users) > 1:
                raise User.TooManyObjects
            if len(users) == 0:
                raise User.DoesNotExist
            return users[0]
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628

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

<<<<<<< HEAD
    def __init__(self, log=None, secret="no_secret", token_expires=604800, algorithm='HS256'): 
=======
    def __init__(self, log=None, secret="no_secret", token_expires=604800, algorithm='HS256', config_dir="."): 
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
        self.JWT_SECRET = secret
        self.JWT_ALGORITHM = algorithm
        self.JWT_EXP_DELTA_SECONDS = token_expires
        # TODO/CHEESE - Thumbnails should have security but some non-authenticated API such
        # as the homekit camera and jukebox may be using it
<<<<<<< HEAD
        self.whitelist=['/eventgateway/activate','/devices','/activate', '/client','/favicon.ico','/login','/logout','/thumbnail','/fonts']
        self.log=log
        self.instance_id=str(uuid.uuid1())
=======
        self.whitelist=['/auth','/video','/devices', '/client','/favicon.ico','/login','/logout','/thumbnail','/fonts']
        self.log=log
        self.instance_id=str(uuid.uuid1())
        self.config_dir=config_dir
        self.refresh_tokens=self.load_json('refresh_tokens')
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628

    # Start - This is the JWT testing code
    async def middleware(self, app, handler):

        async def token_check(request):
            
<<<<<<< HEAD
            #if str(request.rel_url)=="/":
            #    return await handler(request)                
=======
            if str(request.rel_url)=="/":
                return await handler(request)                
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
            
            for item in self.whitelist:
                if str(request.rel_url).startswith(item):
                    return await handler(request)
            
            if not request.method=='OPTIONS':
<<<<<<< HEAD
                request.api_consumer = None
=======
                request.user = None
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
                try:
                    jwt_token = request.headers.get('authorization', None)
                except:
                    self.log.error('.! could not get jwt token from authorization header', exc_info=True)
                if not jwt_token:
                    try:
<<<<<<< HEAD
                        if 'token' in request.cookies:
                            #self.log.info('.. token from cookie: %s' % request.cookies['token'])
                            jwt_token=request.cookies['token']
=======
                        
                        if 'access_token' in request.cookies:
                            #self.log.info('.. token from cookie: %s' % request.cookies['token'])
                            jwt_token=request.cookies['access_token']
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
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
<<<<<<< HEAD
                                if hcookie.split('=')[0]=='token':
=======
                                if hcookie.split('=')[0]=='access_token':
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
                                    jwt_token=hcookie.split('=')[1]
                    except:
                        self.log.error('Could not decipher token from header cookies', exc_info=True)

                if jwt_token:
                    try:
                        payload = jwt.decode(jwt_token, self.JWT_SECRET,
                                             algorithms=[self.JWT_ALGORITHM])
                    except (jwt.DecodeError, jwt.ExpiredSignatureError):
<<<<<<< HEAD
                        self.log.warn('.- Token is invalid. Path: %s' % request.rel_url)
                        raise web.HTTPUnauthorized()
                        #return self.json_response({'message': 'Token is invalid'}, status=400)
                    
                    if 'instance' not in payload or payload['instance']!=self.instance_id:
=======
                        self.log.warn('.- Token is invalid for user. Path: %s' % request.rel_url, exc_info=True)
                        self.log.info('token was: %s' % jwt_token)
                        raise web.HTTPUnauthorized()
                        #return self.json_response({'message': 'Token is invalid'}, status=400)
                    
                    if 'session' not in payload or 'instance' not in payload or payload['instance']!=self.instance_id:
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
                        self.log.debug('.- Token not correct for this instance. Path: %s' % request.rel_url)
                        #self.log.warn('-- Headers: %s' % request.headers)
                        raise web.HTTPUnauthorized()
                    
                    try:
<<<<<<< HEAD
                        request.api_consumer = payload['name']
                        if 'collector' in payload:
                            request.collector = payload['collector']
                        else:
                            request.collector = False

                    except:
                        self.log.error('.. error dealing with payload: %s' % payload, exc_info=True)
                else:
                    self.log.warn('.- No token available for api consumer. Path: %s' % request.rel_url)
=======
                        if 'name' not in payload:
                            request.user = User.objects.get(id=payload['user_id'])
                            request.collector = True
                            request.session = payload['session']
                        else:
                            request.user = payload['name']
                            request.session = payload['session']
                            if 'collector' in payload:
                                request.collector = payload['collector']
                            else:
                                request.collector = False
                    except:
                        self.log.error('.. error dealing with payload: %s' % payload, exc_info=True)
                else:
                    self.log.warn('.- No token available for user. Path: %s' % request.rel_url)
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628
                    #self.log.warn('-- Headers: %s' % request.headers)
                    raise web.HTTPUnauthorized()
                    #return self.json_response({'message': 'Token is missing'}, status=400)
            
            return await handler(request)
            
        return token_check
<<<<<<< HEAD
        
=======
    
    def create_refresh_token(self, user):
        try:
            refresh_id=str(uuid.uuid1())
            payload = {
                "refresh_id": refresh_id,
                "user_id": user.id,
                "user_name": user.login,
                "created": datetime.datetime.utcnow().isoformat()
            }
            jwt_token = jwt.encode(payload, self.JWT_SECRET, self.JWT_ALGORITHM)
            
            self.refresh_tokens=self.load_json('refresh_tokens')
            self.refresh_tokens[refresh_id]=payload
            self.save_json('refresh_tokens', self.refresh_tokens)
            
            return jwt_token.decode('utf-8')
        except:
            self.log.error('!! error creating refresh token', exc_info=True)


    def create_access_token(self, refresh_token):
        try:
            refresh_token_data = jwt.decode(refresh_token, self.JWT_SECRET,
                                 algorithms=[self.JWT_ALGORITHM])

            refresh_id=str(uuid.uuid1())
            payload = {
                "refresh_token_id": refresh_token_data["refresh_id"],
                "user_id": refresh_token_data["user_id"],
                "expires": (datetime.datetime.utcnow() + datetime.timedelta(seconds=self.JWT_EXP_DELTA_SECONDS)).isoformat(),
                "instance": self.instance_id, 
                "session": str(uuid.uuid1())
            }
            jwt_token = jwt.encode(payload, self.JWT_SECRET, self.JWT_ALGORITHM)
            return jwt_token.decode('utf-8')
        except:
            self.log.error('!! error creating refresh token', exc_info=True)

    
    async def get_token_from_credentials(self, username, password):
        try:
            try:
                user = User.objects.get(login=username)
                user.match_password(password)
            
            except User.DoesNotExist:
                self.log.info('.. incorrect user: %s' % username)
                return False
                #return self.json_response({'message': 'User does not exist'}, status=400)
            except User.PasswordDoesNotMatch:
                self.log.info('.. incorrect password: %s' % password)
                return False
                #return self.json_response({'message': 'Incorrect password'}, status=400)
            
            refresh_token=self.create_refresh_token(user)
            access_token=self.create_access_token(refresh_token)
            #self.log.info('generated token %s' % jwt_token)
            #return self.json_response({'token': jwt_token.decode('utf-8')})
            return {"refresh_token": refresh_token, "access_token": access_token, "expires_in": self.JWT_EXP_DELTA_SECONDS }
        except:
            self.log.error('!! error with login post', exc_info=True)
            return False

    async def get_token_from_refresh(self, username, refresh_token):
        try:
            try:
                user = User.objects.get(login=username)
            
            
            # TODO/CHEESE - need to actually store and check refresh certificates
            
            except User.DoesNotExist:
                self.log.info('.. incorrect user: %s' % username)
                return False
                #return self.json_response({'message': 'User does not exist'}, status=400)

            payload = {
                "user_id": user.id,
                "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=self.JWT_EXP_DELTA_SECONDS),
                "instance": self.instance_id,
                "session": str(uuid.uuid1())
            }
            jwt_token = jwt.encode(payload, self.JWT_SECRET, self.JWT_ALGORITHM)
            #self.log.info('generated token %s' % jwt_token)
            #return self.json_response({'token': jwt_token.decode('utf-8')})
            return jwt_token.decode('utf-8')
        except:
            self.log.error('!! error with login post', exc_info=True)
            return False


    async def get_refresh_token_from_credentials(self, username, password):
        try:
            try:
                user = User.objects.get(login=username)
                user.match_password(password)
            
            except User.DoesNotExist:
                self.log.info('.. incorrect user: %s' % username)
                return False
                #return self.json_response({'message': 'User does not exist'}, status=400)
            except User.PasswordDoesNotMatch:
                self.log.info('.. incorrect password: %s' % password)
                return False
                #return self.json_response({'message': 'Incorrect password'}, status=400)

            payload = {
                "user_id": user.id,
                "issued": datetime.datetime.utcnow(),
            }
            
            jwt_token = jwt.encode(payload, self.JWT_SECRET, self.JWT_ALGORITHM)
            if username not in self.refresh_tokens:
                self.refresh_tokens[username]=[]
            self.refresh_tokens[username].append('jwt_token')
            
            self.save_json('refresh_tokens')

            #self.log.info('generated token %s' % jwt_token)
            #return self.json_response({'token': jwt_token.decode('utf-8')})
            return jwt_token.decode('utf-8')
        except:
            self.log.error('!! error with login post', exc_info=True)
            return False

>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628

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
<<<<<<< HEAD
=======
            
    def load_json(self, jsonfilename):
        
        try:
            with open(os.path.join(self.config_dir, '%s.json' % jsonfilename),'r') as jsonfile:
                return json.loads(jsonfile.read())
        except FileNotFoundError:
            self.log.error('!! Error loading json - file does not exist: %s' % jsonfilename)
            return {}
        except:
            self.log.error('Error loading pattern: %s' % jsonfilename,exc_info=True)
            return {}
            
    def save_json(self, jsonfilename, data):
        
        try:
            jsonfile = open(os.path.join(self.config_dir, '%s.json' % jsonfilename), 'wt')
            json.dump(data, jsonfile, ensure_ascii=False, default=self.jsonDateHandler)
            jsonfile.close()

        except:
            self.log.error('Error saving json: %s' % jsonfilename,exc_info=True)
            return {}
            
    def jsonDateHandler(self, obj):

        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            self.log.error('Found unknown object for json dump: (%s) %s' % (type(obj),obj))
            return None
>>>>>>> 82ff6ff7142330074103d86e00f7be05116d7628


