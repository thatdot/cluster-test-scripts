import random, string
from typing import Optional

class Datagen(object):
	def __init__(self, seed=0, expected_count = 1000*1000*1000):
		
		self.seed = int(seed)
		self.r = random.Random(seed)
		self.count = 0
		expected_count = max(expected_count, 1000000)

		# dataset features:
		# Each IP-Host pair is unique (1:1) -- trivial cardinality data
		# Each Site is connected to approx 1K hosts -- low cardinality data
		# Each OU is connected to approx 100K hosts -- high cardinality data, borderline supernode
		# Each customer is connected to approx 1M hosts -- supernode
		self.choices = {
			"customer_id" : [self.rand_string(32, string.ascii_letters) for i in range(expected_count//1000000)],
			"sensor_id"   : [self.rand_string(32, string.ascii_letters) for i in range(2000)],
			# set in next() to sequential integers: # "entity_id"
			"hostname"    : [self.rand_string(9, string.ascii_letters) for i in range(2000)],
			"platform"    : [self.rand_string(15, string.ascii_letters) for i in range(12)],
			"mac_address" : [self.rand_string(17, string.ascii_letters) for i in range(200000)],
			# set in next() to a unique random string: # "local_ip"
			"ou"          : [self.rand_string(8, string.ascii_letters) for i in range(expected_count//100000)],
			"site_name"   : [self.rand_string(18, string.ascii_letters) for i in range(expected_count//1000)]
		}


	def rand_string(self, length, from_chars):
		return ''.join(self.r.choice(from_chars) for i in range(length))

	def __iter__(self):
		return self

	# Python 3 compatibility
	def __next__(self):
		return self.next()

	def next(self):
		def get_and_inc():
			x = self.count
			self.count = self.count + 1
			return x

		result = {k : self.r.choice(self.choices[k]) for k in self.choices.keys()}
		result['entity_id'] = str(get_and_inc() + self.seed)
		result['local_ip'] = self.rand_string(15, string.ascii_letters)
		return result
		# raise StopIteration()

	def take(self, count):
		return [self.next() for i in range(count)]

	# Skip to the specified index and generate that datum
	# If the datum at that index was already generated, return None instead
	def skip_to(self, count) -> Optional[dict]:
		if count <= self.count:
			return None
		else:
			diff = count - self.count
			last = None
			for _ in range(diff):
				last = self.next()
			return last