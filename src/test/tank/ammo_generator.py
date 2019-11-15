import abc
import argparse
import gzip
import itertools
import random
import shutil
import string
import uuid

import durations
import enum
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

_random_body_source = list(string.ascii_uppercase + string.digits)


def replace_percent(replace_in, replace_to, percent):
    result = replace_in[:]
    replace_amount = int(len(result) * percent)
    for _ in range(replace_amount):
        replace_idx = random.randint(0, len(result) - 1)
        to_idx = random.randint(0, len(replace_to) - 1)
        result[replace_idx] = replace_to[to_idx]
    return result


def generate_random_body():
    return ''.join(
        random.choice(_random_body_source)
        for _ in range(random.randint(10, 100))
    )


def generate_unique_keys(amount):
    keys = [
        str(uuid.uuid1())
        for _ in tqdm.trange(amount, desc='Generate entities')
    ]
    random.shuffle(keys)
    return keys


class AmmoGenerator(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, requests):
        self.requests_amount = int(requests)
        self.entities = None
        self.entities_file_name = None

    def get_filler_data(self):
        return self.entities, self.entities_file_name

    @abc.abstractproperty
    def name(self):
        pass

    def get_headers(self):
        return ['[Connection: Keep-Alive]\n']

    def get_path(self):
        return '/v0/entity'

    def make_requests(self, builder, entities=None):
        if entities is None:
            entities = self.entities
        return (
            self.make_single_request(builder, entity)
            for entity in tqdm.tqdm(entities, desc='making requests')
        )

    def make_single_request(self, builder, entity):
        return builder(id=entity)

    def make_uri(self, **kwargs):
        path = self.get_path()
        params = ['{}={}'.format(key, value) for key, value in kwargs.items()]
        if params:
            path += '?' + '&'.join(params)
        return path

    def make_post(self, **kwargs):
        uri = self.make_uri(**kwargs)
        body = generate_random_body()
        return '{} {}\n{}\n'.format(len(body), uri, body)

    def make_get(self, **kwargs):
        uri = self.make_uri(**kwargs)
        return uri + '\n'

    @abc.abstractmethod
    def generate(self):
        pass

    @staticmethod
    def save_gzip(file_name, data):
        with gzip.open(file_name, 'wb') as f:
            f.writelines(data)

    def save(self, requests):
        data = itertools.chain(self.get_headers(), requests)
        self.entities_file_name = '{}-ammo.txt.gz'.format(self.name)
        self.save_gzip(self.entities_file_name, data)


class AmmoGeneratorWithPrefilling(AmmoGenerator):
    def __init__(self, requests):
        super(AmmoGeneratorWithPrefilling, self).__init__(requests)
        self.filler = None
        self.filler_file = None

    def create_filler(self):
        return generate_unique_keys(self.requests_amount)

    def get_filler_data(self):
        return self.filler, self.filler_file

    def get_filler(self):
        if self.filler is None:
            self.filler = self.create_filler()
        self.save_filler()
        return self.filler[:]

    def save_filler(self):
        file_name = '{}-filler.txt.gz'.format(self.name)
        if self.filler_file is not None:
            shutil.copy(self.filler_file, file_name)
        else:
            self.save_gzip(file_name, self.filler)
        self.filler_file = file_name

    def set_filler(self, entities, file_name):
        self.filler = entities
        self.filler_file = file_name


class CreateUniqueGenerator(AmmoGenerator):
    name = LoadTestingMode.create_unique.value

    def generate(self):
        self.entities = generate_unique_keys(self.requests_amount)
        requests = self.make_requests(self.make_post)
        self.save(requests)


class CreateOverwriteGenerator(AmmoGenerator):
    name = LoadTestingMode.create_overwrite.value

    def generate(self):
        entities = generate_unique_keys(self.requests_amount)
        self.entities = replace_percent(entities, entities, 0.1)
        requests = self.make_requests(self.make_post)
        self.save(requests)


class GetGenerator(AmmoGeneratorWithPrefilling):
    name = LoadTestingMode.get.value

    def generate(self):
        self.entities = self.get_filler()
        random.shuffle(self.entities)
        requests = self.make_requests(self.make_get)
        self.save(requests)


class GetNewGenerator(AmmoGeneratorWithPrefilling):
    name = LoadTestingMode.get_new.value

    def generate(self):
        entities = self.get_filler()
        newest_entities = entities[-int(len(entities) * 0.1):]
        self.entities = replace_percent(entities, newest_entities, 0.5)
        requests = self.make_requests(self.make_get)
        self.save(requests)


class MixedGenerator(AmmoGeneratorWithPrefilling):
    name = LoadTestingMode.mixed.value

    def generate(self):
        create_entities = generate_unique_keys(self.requests_amount // 2)
        get_entities = self.get_filler()
        random.shuffle(get_entities)
        get_entities = get_entities[:self.requests_amount // 2]
        create_requests = self.make_requests(self.make_post, create_entities)
        get_requests = self.make_requests(self.make_get, get_entities)
        requests = self.random_order(create_requests, get_requests)
        self.save(requests)

    def random_order(self, *iterables):
        iterators = [iter(it) for it in iterables]
        while iterators:
            iterator = random.choice(iterators)
            try:
                yield iterator.next()
            except StopIteration:
                iterators.remove(iterator)


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
    generators = [generator(requests) for generator in generators]
    if len(generators) == 1:
        generators[0].generate()
        return

    need_filler = [
        gen
        for gen in generators
        if isinstance(gen, AmmoGeneratorWithPrefilling)
    ]
    filler_provider = [gen
                       for gen in generators
                       if isinstance(gen, CreateUniqueGenerator)
                       ] or need_filler

    if filler_provider:
        filler_provider = filler_provider[0]
    generators = [gen for gen in generators if gen is not filler_provider]
    if filler_provider is not None:
        filler_provider.generate()
        for generator in need_filler:
            filler, filler_file = filler_provider.get_filler_data()
            generator.set_filler(filler, filler_file)
    for generator in generators:
        generator.generate()


generate(parser.parse_args())
