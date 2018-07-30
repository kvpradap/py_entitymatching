===============================
Commands Implemented Using Dask
===============================

Downsampling
------------
.. autofunction:: py_entitymatching.dask_down_sample


Blocking
--------
.. autoclass:: py_entitymatching.DaskAttrEquivalenceBlocker
    :members:
.. autoclass:: py_entitymatching.DaskRuleBasedBlocker
    :members:
.. autoclass:: py_entitymatching.DaskBlackBoxBlocker
    :members:

Extracting Feature Vectors
--------------------------
.. autofunction:: py_entitymatching.dask_extract_feature_vecs


ML-Matchers
-----------
.. autoclass:: py_entitymatching.DaskDTMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.DaskRFMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.DaskSVMMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.DaskNBMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__

.. autoclass:: py_entitymatching.DaskLogRegMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__


.. autoclass:: py_entitymatching.DaskXGBoostMatcher
    :inherited-members:
    :exclude-members: __delattr__, __format__, __getattribute__, __hash__, __reduce__, __reduce_ex__, __repr__, __setattr__, __sizeof__, __str__
