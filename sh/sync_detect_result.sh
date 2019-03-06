#!/bin/sh
begin_time=`date -d today +"%Y%m%d%H" --date="-19 hour"`
end_time=`date -d today +"%Y%m%d%H" `
echo $begin_time
echo $end_time
main()
{
  python3 record_syncStatus.py --beginTime $begin_time --endTime $end_time 
}

main "$@"
