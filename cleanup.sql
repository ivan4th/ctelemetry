begin transaction;
delete from event_values where event_id in (select id from events where topic_id in (select id from topics where topic like '%/meta/%' or topic in ('/fionbio/devices/wb-w1/controls/00042d40ffff', '/petr/events/c')));
delete from events where topic_id in (select id from topics where topic like '%/meta/%' or topic in ('/fionbio/devices/wb-w1/controls/00042d40ffff', '/petr/events/c'));
delete from topic_cells where topic_id in (select id from topics where topic like '%/meta/%' or topic in ('/fionbio/devices/wb-w1/controls/00042d40ffff', '/petr/events/c'));
delete from topics where topic like '%/meta/%' or topic in ('/fionbio/devices/wb-w1/controls/00042d40ffff', '/petr/events/c');
commit transaction;
vacuum;
