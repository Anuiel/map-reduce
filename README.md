# Compgrapg package

Project from [Yandex SHAD](https://dataschool.yandex.com/).

We define table as sequence of dictionaries and computation graph as s sequence of some computations.

An example of a computation graph is:

```python
graph = Graph.graph_from_iter('texts') \
    .map(operations.FilterPunctuation('text')) \
    .map(operations.LowerCase('text')) \
    .map(operations.Split('text')) \
    .sort(['text']) \
    .reduce(operations.Count('count'), ['text']) \
    .sort(['count', 'text'])
```

which calculates a number of words in table.

Python 3.11.5

## How to install
```bash
pip3 wheel --wheel-dir dist .

pip3 install . --prefer-binary --force-reinstall --find-links dist
```

### Run tests
```bash
pytest
```

## Examples

For test data unzip `resources/extract_me.tgz`

### Word count
Given a table with format `{'doc_id': ..., 'text :...'}` for every word in `text` column calculate a number of appearances.

For test look up `resources/text_corpus.txt` (after unziping `extract_me.tgz`)

### Tf-Idf
Same table with format `{'doc_id': ..., 'text :...'}`. For every word calculate [tf-idf](https://ru.wikipedia.org/wiki/TF-IDF). For every pair (word, document) 
define Tf-Idf as

```
TFIDF(word_i, doc_j) = (frequency of word_i in doc_j) * ln((total number of docs) / (docs where word_i is present))
```
After for each word output top-3 documents by Tf-Idf metric

For test look up `resources/text_corpus.txt` (after unziping `extract_me.tgz`)


### Pointwise mutual information

Same table format `{'doc_id': ..., 'text :...'}`. For every document output top-10 words by [Pointwise mutual information](https://en.wikipedia.org/wiki/Pointwise_mutual_information).

Also filter irrelevant words with less that 4 letters or less that 2 appearances in document.

For test look up `resources/text_corpus.txt` (after unziping `extract_me.tgz`)


```
pmi(word_i, doc_j) = ln((frequency of word_i in doc_j) / (frequency of word_i in all documents combined))
```

### Average moving speed

In this task, you need to work with information about the movement of people in cars along some subset of the streets of the city of Moscow.

The streets of the city are set as a graph, and information about movement is set as a table, in each row of which data of the form
```
{'edge_id': '624', 'enter_time': '20170912T123410.1794', 'leave_time': '20170912T123412.68'}
```
where `edge_id` is the identifier of the edge of the road graph (that is, just a section of some street), and `enter_time` and `leave_time` are
accordingly, the time of entry and exit from /to this edge (time in UTC).

You are also given an auxiliary table of the form
```
{'edge_id': '313', 'length':121, 'start': [37.31245, 51.256734], 'end': [37.31245, 51.256734]}
```
where `length` is the length in meters, `start` and `end` are the coordinates of the beginning and end of the edge, specified in the format `('lon', 'lat')`.
Maybe not all the edges of the graph have all the meta information, so you should look for the distance yourself.

Note: The distance between the points is suggested to be searched using [haversine distance](https://en.wikipedia.org/wiki/Haversine_formula ), the radius of the Earth should be set equal to 6373 km (check the tests).

Using this information, you need to build a table with the average speed of movement in the city in km/h
depending on the hour and day of the week; these two parameters need to be pulled from enter_time
(this is important because the hour and even the day of the week may change during the journey):
```
{'weekday': 'Mon', 'hour': 4, 'speed': 44.812}
```

For verification, it is useful to build a graph using this table, it should look predictable.
(if you show us, we will give you bonus points, for this you need to attach a graph to the merjrequest).

Files for this task: `resources/travel_times.txt ` and `resources/road_graph_data.txt ` (after unziping `extract_me.tgz`)

**P.S.** see [`visual_yandex_maps.ipynb`](visual_yandex_maps.ipynb) for visual answer

### Run examples
```bash
unzip resources/extract_me.tgz

python3 examples/<example_file>.py --help

python3 examples/<example_file>.py --input <input.txt> --output <output.txt>
```
