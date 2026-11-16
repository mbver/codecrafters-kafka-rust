- Message Size
  ↓ Response Header (v1)
  - Correlation ID
  - Tag Buffer

  ↓ Produce Response Body (v11)
  ↓ Topics Array - Array Length

      ↓ Topic #1
        - Topic Name (Compact String)

        ↓ Partitions Array
          - Array Length

          ↓ Partition #0
            - Partition ID
            - Error Code
            - Base Offset
            - Log Append Time
            - Log Start Offset

            ↓ Record Errors Array
              - Array Length

              ↓ Record Error #0
                - Batch Index
                - Batch Index Error Message (Compact Nullable String)
                - Tag Buffer

            - Error Message (Compact Nullable String)
            - Tag Buffer (current_leader)

        - Tag Buffer (topic)

      ↓ Topic #2
        ...

  - Throttle Time
  - Tag Buffer (response)
