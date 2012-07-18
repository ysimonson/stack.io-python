import zerorpc

REGISTRAR_ENDPOINT = "tcp://127.0.0.1:27615"

class ServiceConfig(object):
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.client = None
        self.introspected = None

class StackIO(object):
    def __init__(self, **options):
        self.options = options

        registrar_client = zerorpc.Client(self.options)
        registrar_client.connect(self.options.get('registrar') or REGISTRAR_ENDPOINT)

        service_endpoints = registrar_client.services(True)

        self._services = dict([(name, ServiceConfig(endpoint)) for name, endpoint in service_endpoints.iteritems()])

        registrar_client.close()

    def expose(self, service_name, endpoint, context):
        server = zerorpc.Server(context)
        server.bind(endpoint)

        registrar = self.use("registrar")
        registrar.register(service_name, endpoint)

        server.run()

    def services(self):
        return self._services.keys()

    def introspect(self, service_name):
        cached = self._services.get(service_name)

        if cached == None:
            raise Exception("Unknown service")
        elif cached.introspected != None:
            return cached.introspected
        else:
            client = self.use(service_name)
            cached.introspected = client._zerorpc_inspect()
            return cached.introspected

    def use(self, service_name):
        cached = self._services.get(service_name)

        if cached == None:
            raise Exception("Unknown service")
        elif cached.client != None:
            return cached.client
        else:
            cached.client = zerorpc.Client()
            cached.client.connect(cached.endpoint)
            return cached.client

