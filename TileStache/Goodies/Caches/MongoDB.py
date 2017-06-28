import sys

try:
    from pymongo import MongoClient
    from bson.binary import Binary
except ImportError:
    # at least we can build the documentation
    pass


def tile_key(layer, coord, format):
    """ Return a tile key string.
    """
    name = layer.name()
    tile = '%(zoom)d/%(column)d/%(row)d' % coord.__dict__
    key = str('%(name)s/%(tile)s.%(format)s' % locals())
    return key


class Cache:

    def __init__(self, host, port=27017):
        self.conn = MongoClient(host, port)

    def lock(self, layer, coord, format):
        sys.stderr.write('lock %d/%d/%d, %s\n' % (coord.zoom, coord.column,
                                                  coord.row, format))

    def unlock(self, layer, coord, format):
        sys.stderr.write('unlock %d/%d/%d, %s\n' % (coord.zoom, coord.column,
                                                    coord.row, format))

    def remove(self, layer, coord, format):
        """ Remove a cached tile.
        """
        # TODO: write me
        raise NotImplementedError(
            'MongoDB Cache does not yet implement the .remove() method.')

    def read(self, layer, coord, format):
        """ Read a cached tile.
        """
        sys.stderr.write('read %d/%d/%d, %s\n' % (coord.zoom, coord.column,
                                                  coord.row, format))
        key = tile_key(layer, coord, format)
        item = self.conn.cache.tiles.find_one({'_id': key})

        if item:
            sys.stderr.write('...hit\n')
            return item['buffer']

        sys.stderr.write('...miss\n')
        return None

    def save(self, body, layer, coord, format):
        """
        """
        sys.stderr.write('save %d/%d/%d, %s\n' % (coord.zoom, coord.column,
                                                coord.row, format))
        key = tile_key(layer, coord, format)
        self.conn.cache.tiles.update_one({'_id': key}, {'$set': {'buffer': Binary(body)}}, upsert=True)
