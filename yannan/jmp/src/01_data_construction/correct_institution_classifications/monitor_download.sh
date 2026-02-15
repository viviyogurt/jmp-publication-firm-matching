#!/bin/bash
# Monitor the institutions download progress

echo "======================================================================"
echo "OpenAlex Institutions Download Progress Monitor"
echo "======================================================================"
echo ""

FILE="/home/kurtluo/yannan/jmp/data/raw/publication/institutions_all.gz"

if [ -f "$FILE" ]; then
    # Get file size in MB
    SIZE=$(ls -lh "$FILE" | awk '{print $5}')
    echo "File size: $SIZE"

    # Count institutions (handle incomplete gzip)
    LINES=$(zcat "$FILE" 2>/dev/null | wc -l)
    echo "Institutions downloaded: $LINES / 120,658"
    echo "Progress: $(echo "scale=1; $LINES / 120658 * 100" | bc)%"

    if [ $LINES -eq 120658 ]; then
        echo ""
        echo "✅ DOWNLOAD COMPLETE!"
        echo ""
        echo "You can now run:"
        echo "  python src/01_data_construction/correct_institution_classifications/classify_fast.py process 10000"
    else
        echo ""
        echo "⏳ Still downloading..."
        echo "Estimated time remaining: ~20-30 minutes"
    fi
else
    echo "❌ Download file not found"
fi

echo ""
echo "======================================================================"
