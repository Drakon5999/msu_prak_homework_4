import collections
import enum
import logging
from pycorenlp import StanfordCoreNLP
from .constants import *
logger = logging.getLogger(__name__)

TWIT_ID = 'Tweet Id'
AUTHOR = 'Nickname'
CONTENT = 'Tweet content'
RETWITS = 'RTs'
COUNTRY = 'Country'
TWIT_URL = 'Tweet Url'


class TwitsStatCalculator:
    def __init__(self, twits):
        self.twits = twits

    def get_top_words(self):
        words = collections.defaultdict(int)
        for twit in self.twits:
            content = twit[CONTENT].split()
            for word in content:
                if len(word) > 2:
                    words[word] += 1
        return collections.OrderedDict(
            sorted(words.items(), key=lambda kv: -kv[1])[:10]
        )

    def get_top_twits(self):
        logger.debug(self.twits[0])
        return sorted(self.twits, key=lambda k: -int(k[RETWITS]) if k[RETWITS] else 0)[:10]

    def get_top_authors(self):
        authors = collections.defaultdict(int)
        for twit in self.twits:
            authors[twit[AUTHOR]] += twit[RETWITS] if twit[RETWITS] else 0
        return collections.OrderedDict(
            sorted(authors.items(), key=lambda kv: -kv[1])[:10]
        )

    def get_top_country_retwits(self):
        countries = collections.defaultdict(int)
        for twit in self.twits:
            country = twit[COUNTRY] if twit[COUNTRY] else "UNKNOWN"
            countries[country] += twit[RETWITS] if twit[RETWITS] else 0

        return collections.OrderedDict(
            sorted(countries.items(), key=lambda kv: -kv[1])[:10]
        )

    def get_top_country_twits(self):
        countries = collections.defaultdict(int)
        for twit in self.twits:
            country = twit[COUNTRY] if twit[COUNTRY] else "UNKNOWN"
            countries[country] += 1

        return collections.OrderedDict(
            sorted(countries.items(), key=lambda kv: -kv[1])[:10]
        )

    def get_full_report(self):
        return {
            'top_words': self.get_top_words(),
            'top_twits': self.get_top_twits(),
            'top_authors': self.get_top_authors(),
            'top_country_twits': self.get_top_country_twits(),
            'top_country_retwits': self.get_top_country_retwits()
        }


class EntityExtractor:
    def __init__(self, twits):
        self.nlp = StanfordCoreNLP('http://localhost:9000')
        self.twits = twits

    def extract_entities(self):
        entities = []
        for twit in self.twits:
            entities.append(
                {
                    'ner': self.nlp.annotate(twit[CONTENT], properties={
                       'annotators': 'ner',
                       'outputFormat': 'json',
                       'timeout': 1000,
                    }),
                    TWIT_ID: twit[TWIT_ID]
                 }
            )
        return entities


class DataParser:
    @enum.unique
    class StatesOfGet(enum.Enum):
        S_GET_COMMAND = 0
        S_GET_DATA_SIZE = 1
        S_GET_DATA = 2
        S_GET_FINISH = 3

    class BadCommandException(Exception):
        def __init__(self, command):
            self.command = command

    def __init__(self, commands):
        self._commands = commands
        self._state = self.StatesOfGet(0)
        self.command = None
        self.data_size = None
        self.is_data_ready = False
        self.data_to_process = None
        self._data = b''
        self._max_command_len = max(len(cmd) for cmd in self._commands)

    def parse_command(self):
        logger.debug(self._commands)
        for i in range(min(self._max_command_len + 1, len(self._data))):
            logger.debug(self._data[:i])
            if self._data[:i] in self._commands:
                self.command = self._commands[self._data[:i]]
                self._data = self._data[i:]

    def parse_data(self, data_part):
        self._data += data_part
        if self._state == self.StatesOfGet.S_GET_COMMAND:
            self.parse_command()
            if self.command is None and len(self._data) > self._max_command_len:
                raise self.BadCommandException(self._data[:self._max_command_len])
            elif self.command is not None:
                logger.debug("Get command {}".format(self.command))
                self._state = self.StatesOfGet.S_GET_DATA_SIZE
                logger.debug("state {}".format(self._state))
                return self.parse_data(b'')
        elif self._state == self.StatesOfGet.S_GET_DATA_SIZE:
            if len(self._data) > INT_SIZE:
                self.data_size = int.from_bytes(self._data[:INT_SIZE], byteorder=BYTE_ORDER)
                logger.debug("Get size {}".format(self.data_size))
                self._state = self.StatesOfGet.S_GET_DATA
                self._data = self._data[INT_SIZE:]
                return self.parse_data(b'')
        elif self._state == self.StatesOfGet.S_GET_DATA:
            if len(self._data) >= self.data_size:
                self.data_to_process = self._data[:self.data_size]
                self.is_data_ready = True
                self._state = self.StatesOfGet.S_GET_FINISH
                logger.debug("Recv data len {}".format(len(self.data_to_process)))

        return self.is_data_ready
