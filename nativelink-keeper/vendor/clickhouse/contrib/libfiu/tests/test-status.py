
"""
Test fiu_status(): reports 1 for currently-enabled points and 0 otherwise,
including for FIU_ONETIME points that have already fired.
"""

import fiu

# Not registered yet.
assert not fiu.status('p1')

fiu.enable('p1')
assert fiu.status('p1')

# Other names remain unaffected.
assert not fiu.status('p2')

fiu.disable('p1')
assert not fiu.status('p1')

# Probability-based: still considered enabled even though individual fails roll.
fiu.enable_random('pr', probability=0.0)
assert fiu.status('pr')
fiu.disable('pr')
assert not fiu.status('pr')

# ONETIME: enabled until consumed, then reports 0.
fiu.enable('po', flags=fiu.Flags.ONETIME)
assert fiu.status('po')
assert fiu.fail('po')
assert not fiu.status('po')
assert not fiu.fail('po')
