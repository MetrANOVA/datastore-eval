CREATE DATABASE IF NOT EXISTS metranova;

DROP DICTIONARY IF EXISTS metranova.routers;
CREATE DICTIONARY metranova.routers (
    ip_address IPv6,
    name String
)
PRIMARY KEY ip_address
SOURCE(FILE(path '/var/lib/clickhouse/user_files/dictionaries/router_ips.csv' format 'CSVWithNames'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(HASHED());


DROP DICTIONARY IF EXISTS metranova.router_interfaces;
CREATE DICTIONARY metranova.router_interfaces (
    router_name String,
    if_index UInt16,
    if_name String
)
PRIMARY KEY router_name, if_index
SOURCE(FILE(path '/var/lib/clickhouse/user_files/dictionaries/router_interfaces.csv' format 'CSVWithNames'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());


DROP DICTIONARY IF EXISTS metranova.ip2as;
CREATE DICTIONARY metranova.ip2as (
    prefix String,
    autonomous_system_number UInt16,
    autonomous_system_organization String,
    isp String,
    organization String
)
PRIMARY KEY prefix
SOURCE(FILE(path '/var/lib/clickhouse/user_files/dictionaries/ip2as.csv' format 'CSVWithNames'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(IP_TRIE());



DROP DICTIONARY IF EXISTS metranova.ip2location;
CREATE DICTIONARY metranova.ip2location (
    prefix String,
    city_geoname_id UInt32,
    city_name String,
    continent_code String,
    continent_geoname_id UInt32,
    continent_name String,
    country_geoname_id UInt32,
    country_is_in_european_union Bool,
    country_iso_code String,
    country_name String,
    location_accuracy_radius UInt32,
    location_latitude Float32,
    location_longitude Float32,
    location_metro_code String,
    location_time_zone String,
    postal_code String,
    registered_country_geoname_id UInt32,
    registered_country_is_in_euopean_union Bool,
    registered_country_iso_code String,
    registered_country_name String,
    represented_country_geoname_id UInt32,
    represented_country_is_in_european_union Bool,
    represented_country_iso_code String,
    represented_country_name String,
    represented_country_type String,
    subdivisions_geoname_id UInt32,
    subdivisions_iso_code String,
    subdivisions_name String,
    traits_is_anonymous_proxy Bool,
    traits_is_satellite_provider Bool
)
PRIMARY KEY prefix
SOURCE(FILE(path '/var/lib/clickhouse/user_files/dictionaries/ip2location.csv' format 'CSVWithNames'))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(IP_TRIE());