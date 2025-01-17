import abc
import argparse
import gzip
import random
import string

import durations
import enum
import numpy as np
import tqdm


class LoadTestingMode(enum.Enum):
    create_unique = 'create-unique'
    create_overwrite = 'create-overwrite'
    get = 'get'
    get_new = 'get-new'
    mixed = 'mixed'
    range = 'range'
    all = 'all'

    def __str__(self):
        return self.value

    @staticmethod
    def help_message():
        return """Type of generated ammo:
{create_unique}     Put with unique entities    
{create_overwrite}  Put with duplicated entities (10%% probability)
{get}               Get entities uniformly. Also produce ammo to fulfill storage   
{get_new}           Get mostly newest entities. Also produce ammo to fulfill storage
{mixed}             Mixed get and create entities. Also produce ammo to fulfill storage
{range}             Range requests (1000 entities in a single range)
{all}               Generate all ammo   
        """.format(
            create_unique=LoadTestingMode.create_unique.value,
            create_overwrite=LoadTestingMode.create_overwrite.value,
            get=LoadTestingMode.get.value,
            get_new=LoadTestingMode.get_new.value,
            mixed=LoadTestingMode.mixed.value,
            range=LoadTestingMode.range.value,
            all=LoadTestingMode.all.value
        )


parser = argparse.ArgumentParser(
    description='Generate ammo with specified parameters',
    formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--rps', type=int, required=True,
                    help='Expected maximal RPS')
parser.add_argument('--mode', type=LoadTestingMode,
                    choices=list(LoadTestingMode), required=True,
                    help=LoadTestingMode.help_message())
parser.add_argument('--duration', type=durations.Duration, required=True,
                    help='Expected maximal duration')
parser.add_argument('--nodes', type=int, required=True,
                    help='Number of nodes in testing cluster')
parser.add_argument('--filler', type=int, required=False,
                    help=("Optional: storage prefilling data amount."
                          " Default: same is requests amount"))


def generate_random_body_pool():
    _random_body_source = list(string.ascii_uppercase + string.digits)
    return [
        ''.join(
            random.choice(_random_body_source)
            for _ in range(random.randint(10, 100))
        )
        for _ in range(100)
    ]


body_pool = generate_random_body_pool()


def generate_random_body():
    return random.choice(body_pool)


def generate_unique_keys(amount, template=''):
    return np.asarray([
        template.format(i) if template else str(i)
        for i in range(amount)
    ])


class AmmoGenerator(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, requests, nodes):
        self.requests_amount = int(requests)
        self.nodes = int(nodes) or 1

    @abc.abstractproperty
    def name(self):
        pass

    def get_data(self):
        return generate_unique_keys(self.requests_amount)

    def get_path(self):
        return '/v0/entity'

    @abc.abstractmethod
    def build_request(self, data):
        pass

    def make_requests(self, entities):
        return (
            self.build_request(entity)
            for entity in
            tqdm.tqdm(entities, desc='Generate {} requests'.format(self.name))
        )

    def make_request_path(self, path=None, **kwargs):
        if path is None:
            path = self.get_path()
        params = ['{}={}'.format(key, value) for key, value in kwargs.items()]
        if params:
            path += '?' + '&'.join(params)
        return path

    def get_headers(self):
        return '\n'.join([
            'User-Agent: yandex-tank'
        ])

    def make_put(self, path=None, **kwargs):
        req_template = (
            "PUT {path} HTTP/1.1\r\n"
            "{headers}\r\n"
            "Content-Length: {body_length}\r\n"
            "\r\n"
            "{body}"
        )
        request_path = self.make_request_path(path, **kwargs)
        body = generate_random_body()
        headers = self.get_headers()
        request = req_template.format(
            path=request_path, body=body, body_length=len(body), headers=headers
        )
        return self.wrap_request(request, tag='put')

    def make_get(self, path=None, **kwargs):
        req_template = (
            "GET {path} HTTP/1.1\r\n"
            "{headers}\r\n\r\n"
        )
        path = self.make_request_path(path, **kwargs)
        request = req_template.format(path=path, headers=self.get_headers())
        return self.wrap_request(request, tag='get')

    @staticmethod
    def wrap_request(request, tag=''):
        size = len(request)
        return '{size} {tag}\n{request}\r\n'.format(
            size=size, tag=tag, request=request
        )

    @abc.abstractmethod
    def generate(self):
        pass

    @staticmethod
    def save_gzip(file_name, data):
        with gzip.open(file_name, 'wb') as f:
            f.writelines(data)

    def save(self, requests):
        file_name = '{}-ammo.gz'.format(self.name)
        self.save_gzip(file_name, requests)


class AmmoGeneratorWithPrefilling(AmmoGenerator):
    __metaclass__ = abc.ABCMeta

    def __init__(self, requests, nodes, filler_size):
        super(AmmoGeneratorWithPrefilling, self).__init__(requests, nodes)
        self.filler_size = filler_size or self.requests_amount

    def get_filler_data(self):
        return generate_unique_keys(self.filler_size)

    def get_filler(self):
        filler_entities = self.get_filler_data()
        replicas = '{}/{}'.format(self.nodes, self.nodes)
        requests = (
            self.make_put(path='/v0/entity', id=key, replicas=replicas)
            for key in
            tqdm.tqdm(filler_entities, desc='Generate filler for ' + self.name)
        )
        self.save_filler(requests)
        return filler_entities

    def save_filler(self, data):
        file_name = '{}-filler.gz'.format(self.name)
        self.save_gzip(file_name, data)


class CreateUniqueGenerator(AmmoGenerator):
    name = LoadTestingMode.create_unique.value

    def build_request(self, data):
        return self.make_put(id=data)

    def generate(self):
        entities = self.get_data()
        requests = self.make_requests(entities)
        self.save(requests)


class CreateOverwriteGenerator(CreateUniqueGenerator):
    name = LoadTestingMode.create_overwrite.value

    def generate(self):
        entities = self.get_data()
        entities = self.replace_percent(entities, entities, 0.1)
        requests = self.make_requests(entities)
        self.save(requests)

    @staticmethod
    def replace_percent(replace_in, replace_to, percent):
        result = replace_in[:]
        replace_amount = int(len(result) * percent)
        for _ in range(replace_amount):
            replace_idx = random.randint(0, len(result) - 1)
            to_idx = random.randint(0, len(replace_to) - 1)
            result[replace_idx] = replace_to[to_idx]
        return result


class GetGenerator(AmmoGeneratorWithPrefilling):
    name = LoadTestingMode.get.value

    def build_request(self, data):
        return self.make_get(id=data)

    def generate(self):
        entities = self.get_filler()
        get_entities = np.random.choice(entities, self.requests_amount)
        requests = self.make_requests(get_entities)
        self.save(requests)


class GetNewGenerator(GetGenerator):
    name = LoadTestingMode.get_new.value

    def generate(self):
        entities = self.get_filler()
        distribution = np.random.exponential(scale=2, size=len(entities))
        weight = distribution / np.sum(distribution)
        weight = weight[::-1]
        get_entities = np.random.choice(entities, size=self.requests_amount,
                                        p=weight)
        requests = self.make_requests(get_entities)
        self.save(requests)


class MixedGenerator(AmmoGeneratorWithPrefilling):
    name = LoadTestingMode.mixed.value

    GET = 'get'
    POST = 'post'
    CHOICES = [GET, POST]

    def build_request(self, data):
        _type, entity = data
        if _type == self.GET:
            return self.make_get(id=entity)
        else:
            return self.make_put(id=entity)

    def generate(self):
        create_entities = generate_unique_keys(self.requests_amount // 2,
                                               template='mixed_{}')
        get_entities = self.get_filler()

        def sample():
            _type = np.random.choice(self.CHOICES)
            if _type == self.GET:
                entity = np.random.choice(get_entities)
            else:
                entity = np.random.choice(create_entities)
            return _type, entity

        samples = (sample() for _ in range(self.requests_amount))
        requests = self.make_requests(samples)
        self.save(requests)


class RangeGenerator(AmmoGeneratorWithPrefilling):
    name = LoadTestingMode.range.value
    RANGE_SIZE = 1000

    def get_path(self):
        return '/v0/entities'

    def get_filler_data(self):
        return generate_unique_keys(self.filler_size,
                                    template='entity_{:010d}')

    def build_request(self, data):
        start, end = data
        return self.make_get(start=start, end=end)

    def generate(self):
        entities = self.get_filler()
        max_start_idx = max(0, len(entities) - self.RANGE_SIZE - 1)
        starts = np.random.random_integers(0, max_start_idx,
                                           self.requests_amount)
        ranges = [
            (entities[start], entities[start + self.RANGE_SIZE])
            for start in starts
        ]
        requests = self.make_requests(ranges)
        self.save(requests)


def generators_for_mode(mode):
    generator_for_mode = {
        LoadTestingMode.create_unique: CreateUniqueGenerator,
        LoadTestingMode.create_overwrite: CreateOverwriteGenerator,
        LoadTestingMode.get: GetGenerator,
        LoadTestingMode.get_new: GetNewGenerator,
        LoadTestingMode.mixed: MixedGenerator,
        LoadTestingMode.range: RangeGenerator,
    }
    if mode == LoadTestingMode.all:
        return list(generator_for_mode.values())
    else:
        return [generator_for_mode[mode]]


def generate(args):
    generators = generators_for_mode(args.mode)
    requests = args.duration.to_seconds() * args.rps
    for generator in generators:
        if issubclass(generator, AmmoGeneratorWithPrefilling):
            generator(requests, args.nodes, args.filler).generate()
        else:
            generator(requests, args.nodes).generate()


generate(parser.parse_args())
