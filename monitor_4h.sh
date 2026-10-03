#!/bin/bash
echo "=== 4 hour silent resource monitor ===" > /tmp/monitor_4h.txt
echo "Start: $(date)" >> /tmp/monitor_4h.txt
echo "Interval: 10min, Total: 4h(24 samples)" >> /tmp/monitor_4h.txt
echo "" >> /tmp/monitor_4h.txt
for i in $(seq 1 24); do
  echo "=== Sample #$i / 24 ===" >> /tmp/monitor_4h.txt
  echo "Time: $(date +%H:%M:%S)" >> /tmp/monitor_4h.txt
  top -bn1 | head -10 >> /tmp/monitor_4h.txt
  echo "---" >> /tmp/monitor_4h.txt
  df -h >> /tmp/monitor_4h.txt
  echo "" >> /tmp/monitor_4h.txt
  if [ $i -lt 24 ]; then
    sleep 600
  fi
done
echo "=== Monitor ended ===" >> /tmp/monitor_4h.txt
echo "End: $(date)" >> /tmp/monitor_4h.txt