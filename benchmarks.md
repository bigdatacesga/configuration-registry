| #nodes (slaves.number)  |  threads      | registry endpoint | time (s) |
|-------------------------|---------------|-------------------|----------|
| 2                       |  sequential   | mesosmaster       | 4        |
| 2                       |  8            | mesosmaster       | 3        |
| 2                       |  16           | mesosmaster       | 3        |
| 2                       |  8            | localhost         | 3        |
| 20                      |  sequential   | mesosmaster       | 19       |
| 20                      |  8            | mesosmaster       | 7.7      |
| 20                      |  32           | mesosmaster       | 7.0      |
| 20                      |  8            | localhost         | 7.2      |
| 20                      |  16           | localhost         | 7.2      |
| 20                      |  32           | localhost         | 7.4      |
| 200                     |  sequential   | mesosmaster       | 121      |
| 200                     |  2            | mesosmaster       | 81       |
| 200                     |  4            | mesosmaster       | 58       |
| 200                     |  8            | mesosmaster       | 50       |
| 200                     |  16           | mesosmaster       | 50       |
