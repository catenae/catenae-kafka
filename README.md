# catenae

## Guide
To-do

## Examples

### Middle link
#### Micromodule definition
```python
from catenae import Link, Electron


class BitcoinFilter(Link):

    def setup(self):
        self.allowed_tokens = set(['bitcoin', 'btc'])

    def transform(self, electron):
        tokens = set(electron.value.split())
        if self.allowed_tokens.intersection(tokens):
            return electron
        
if __name__ == "__main__":
    BitcoinFilter().start()
```

#### Micromodule execution
```
docker run -tid catenae/link \
-v "$(pwd)"/bitcoin_filter.py:/bitcoin_filter.py \
bitcoin_filter.py -i input_stream -o filtered_stream -b localhost:9092
```

## References
> A Micromodule Approach for Building Real-Time Systems with Python-Based Models: Application to Early Risk Detection of Depression on Social Media — R Martínez-Castaño, JC Pichel, DE Losada, F Crestani — European Conference on Information Retrieval, 2018 — [PDF](https://dev.brunneis.com/documents/Papers/ECIR%202018/A%20Micromodule%20Approach%20for%20Building%20Real-Time%20Systems%20with%20Python-Based%20Models:%20Application%20to%20Early%20Risk%20Detection%20of%20Depression%20on%20Social%20Media.pdf)

> Building Python-Based Topologies for Massive Processing of Social Media Data in Real Time — R Martínez-Castaño, JC Pichel, DE Losada — Proceedings of the 5th Spanish Conference on Information Retrieval, 2018
