for i in $(seq 1 500); do
    curl -X POST http://localhost:8000/append/ \
    -H "Content-Type: application/json" \
    -d "{\"data\": {\"request_number\": $i}}" &
done
wait
echo "All requests sent!"