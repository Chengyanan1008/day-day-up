#!/bin/bash
inotify=/usr/local/inotify-tools/bin/inotifywait
$inotify -mrq --timefmt '%d/%m/%y %H:%M' --format '%T %w%f' -e modify,delete,create,close_write /data | while read file
do
  cd / &&
  rsync -avz /data/access.log.*  root@hadoop103:/data
done
