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
{all}               Generate all ammo   
        """.format(
            create_unique=LoadTestingMode.create_unique.value,
            create_overwrite=LoadTestingMode.create_overwrite.value,
            get=LoadTestingMode.get.value,
            get_new=LoadTestingMode.get_new.value,
            mixed=LoadTestingMode.mixed.value,
            all=LoadTestingMode.all.value
        )


parser = argparse.ArgumentParser(
    description='Generate ammo with specified parameters',
    formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--rps', type=int, required=True,
                    help='Expected maximal RPS')
parser.add_argument('--duration', type=durations.Duration, required=True,
                    help='Expected maximal duration')
parser.add_argument('--mode', type=LoadTestingMode,
                    choices=list(LoadTestingMode), required=True,
                    help=LoadTestingMode.help_message())


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


def generate_unique_keys(amount, prefix=''):
    return np.asarray([prefix + str(i) for i in range(amount)])


class AmmoGenerator(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, requests):
        self.requests_amount = int(requests)

    @abc.abstractproperty
    def name(self):
        pass

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

    def make_request_path(self, **kwargs):
        path = self.get_path()
        params = ['{}={}'.format(key, value) for key, value in kwargs.items()]
        if params:
            path += '?' + '&'.join(params)
        return path

    def get_headers(self):
        return '\n'.join([
            'Host: overload.yandex.net'
        ])

    def make_put(self, **kwargs):
        req_template = (
            "PUT {path} HTTP/1.1\r\n"
            "{headers}\r\n"
            "Content-Length: {body_length}\r\n"
            "\r\n"
            "{body}"
        )
        path = self.make_request_path(**kwargs)
        body = generate_random_body()
        headers = self.get_headers()
        request = req_template.format(
            path=path, body=body, body_length=len(body), headers=headers
        )
        return self.wrap_request(request, tag='put')

    def make_get(self, **kwargs):
        req_template = (
            "GET {path} HTTP/1.1\r\n"
            "{headers}\r\n"
        )
        path = self.make_request_path(**kwargs)
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

    def get_filler(self):
        filler_entities = generate_unique_keys(self.requests_amount)
        requests = [
            self.make_put(id=key)
            for key in
            tqdm.tqdm(filler_entities, desc='Generate filler for ' + self.name)
        ]
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
        entities = generate_unique_keys(self.requests_amount)
        requests = self.make_requests(entities)
        self.save(requests)


class CreateOverwriteGenerator(CreateUniqueGenerator):
    name = LoadTestingMode.create_overwrite.value

    def generate(self):
        entities = generate_unique_keys(self.requests_amount)
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
        get_entities = np.random.choice(entities, len(entities))
        requests = self.make_requests(get_entities)
        self.save(requests)


class GetNewGenerator(GetGenerator):
    name = LoadTestingMode.get_new.value

    def generate(self):
        entities = self.get_filler()
        distribution = np.random.exponential(scale=2, size=self.requests_amount)
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
                                               prefix='mixed')
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


def generators_for_mode(mode):
    generator_for_mode = {
        LoadTestingMode.create_unique: CreateUniqueGenerator,
        LoadTestingMode.create_overwrite: CreateOverwriteGenerator,
        LoadTestingMode.get: GetGenerator,
        LoadTestingMode.get_new: GetNewGenerator,
        LoadTestingMode.mixed: MixedGenerator,
    }
    if mode == LoadTestingMode.all:
        return list(generator_for_mode.values())
    else:
        return [generator_for_mode[mode]]


def generate(args):
    generators = generators_for_mode(args.mode)
    requests = args.duration.to_seconds() * args.rps
    for generator in generators:
        generator(requests).generate()


generate(parser.parse_args())
