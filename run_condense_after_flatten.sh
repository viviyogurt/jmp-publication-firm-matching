#!/bin/bash
# Wait for flatten to complete, then run condense

echo "Waiting for flatten script to complete..."
while ps aux | grep -v grep | grep "flatten_ai_papers_batches.py" > /dev/null; do
    sleep 60
    echo "Still flattening... (checking every 60s)"
done

echo "Flatten complete! Starting condense script..."
cd /home/kurtluo/yannan/jmp
nohup python src/01_data_construction/condense_ai_papers_dataset.py --resume > logs/condense_ai_papers_noduplication.log 2>&1 &
echo "Condense script started with PID: $!"
echo "Monitor with: tail -f logs/condense_ai_papers_noduplication.log"
