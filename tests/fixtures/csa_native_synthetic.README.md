# Synthetic CSA FileGDB

`csa_native_synthetic.gdb.zip` contains two invented polygon features, not Census
data. GDAL 3.8.4's OpenFileGDB driver created the dataset, fields, native OBJECTIDs
1 and 2, and EPSG:4269 geometry. Strings have the widths required by the CSA
adapter. All native validation runs against this archive in the lifecycle tests.

The driver omits the OID's `Editable` declaration from its generated XML. The
fixture explicitly declares that intrinsic OID non-editable by updating its
`GDB_Items.Definition` through GDAL's ordinary writable layer API. No source
validator is mocked or relaxed. This fixture implies no dataset acceptance.
