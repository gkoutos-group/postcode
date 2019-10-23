
"""
Class definition for the handling of the main data collector
"""

import time
import pandas as pd
import numpy as np
import os.path
import copy
from integrator.util import ObtainDataError


class DataCollector:
    """
    Main class for data collection, this class will handle all the others.

    @param database_file_handler (function that yields data blocks; string with csv dataset location; or pandas dataframe): the source data
    @param sources: the difference data sources used
    @param reference_sources (list of DBMapping classes): these are the classes that map different reference variables
    @param reference_engines (list of sqlalchemy engines): the engines to be used by the respective list of reference sources
    @param verbose: output some verbose information
    @param chunksize (default 4*4096): the size of the chunk if the input database is a file or a dataframe
    @param stop_after_chunk (default None): if it should stop collecting after a chunk
    """
    def __init__(self, database_handler, sources=None, reference_sources=None, reference_engines=None, verbose=True, chunksize=4*4096, stop_after_chunk=None):
        if type(database_handler) is str: # str input 
            if not os.path.isfile(database_handler):
                raise ObtainDataError("Input file does not exist: '{}'! Or we don't have permission to read.".format(database_handler))
            def _db_get():
                for chunk in pd.read_csv(database_handler, chunksize=chunksize):
                    yield chunk
            database_file_handler = _db_get
        elif isinstance(database_handler, pd.DataFrame): # pandas.DataFrame input
            def _db_get():
                for i, j in database_handler.groupby(np.arange(len(database_handler))//chunksize):
                    yield j
            database_file_handler = _db_get
        else: # function that yields chunks
            database_file_handler = database_handler
        self.database_file_handler = database_file_handler
        if type(sources) is not list: #we are avoiding issues here
            sources = [sources]
        self.sources = copy.copy(sources)
        self.checked = False
        self.verbose = verbose
        if reference_sources and reference_engines: #if we possibly doing mapping we need the same number of reference mappers and engine to use with them
            if len(reference_sources) != len(reference_engines):
                raise ObtainDataError('The references sources and engines have different length.')
            self._reference_engines = {i: j for i, j in zip(reference_sources, reference_engines)}
        else:
            self._reference_engines = None
        self._build_reference_graph(reference_sources)
        self.stop_after_chunk = stop_after_chunk

    def _build_reference_graph(self, reference_sources):
        """
        This method builds a graph for future matching of columns.

        @param reference_sources: the references used for dependency resolution
        """
        # print('- Building reference graph.')
        graph = dict()
        if not reference_sources:
            self.reference_graph = None
            return
        for i in reference_sources:
            for a in i.AVAILABLE:
                if a not in graph:
                    graph[a] = {'neighbours': dict()}
                for b in i.AVAILABLE:
                    # if it is the same variable skip
                    if a == b:
                        continue
                    # check if the variable is valid
                    try:
                        # print('|- {} -> {}'.format(a,b), end=' ')
                        i.check_variables_interaction(from_variable=a, to_variables=b)
                    except Exception as e:
                        # print('fail - ', e)
                        continue
                    # print('ok')
                    # add to the neighbours
                    if b not in graph[a]['neighbours']:
                        graph[a]['neighbours'][b] = list()
                    graph[a]['neighbours'][b].append(i) #format: graph [source] "neighbours" [target] = [methods]
        self.reference_graph = graph

    def _minimum_mapping(self, from_variables, to_variables):
        """
        This creates reference variables mapping using the minimum number of jumps possible.

        @param from_variables: the source variables
        @param to_variables: the variables we need
        """
        graph = self.reference_graph
        cur_elements = [(i, []) for i in from_variables]
        target_paths = dict() #this will map target-paths
        if graph:
            while len(cur_elements) > 0:
                ce, cpath = cur_elements.pop(0)
                if ce not in graph or 'visited' in graph[ce]:
                    continue
                else:
                    graph[ce]['visited'] = True
                for i in graph[ce]['neighbours'].keys():
                    if i in to_variables:
                        if i not in target_paths:
                            target_paths[i] = list()
                        for m in graph[ce]['neighbours'][i]:
                            target_paths[i].append((i, [(ce, m)] + cpath))
                    else:
                        for m in graph[ce]['neighbours'][i]:
                            cur_elements.append((i, [(ce, m)] + cpath))
        we_got = set([i for i in target_paths.keys()]).intersection(set(to_variables))
        if len(we_got) < len(to_variables):
            raise ObtainDataError('Not possible to map all the variables. We got "{}". We need "{}".'.format('", "'.join(we_got), '", "'.join(to_variables)))
        # TODO: the next step can be made more efficient by obtaining the minimum cut, in here we are using the shortest path instead
        currently_available = set()
        target_summarized = dict() #format from_variables, to_variables, method
        for target, options in target_paths.items():
            op_len = sorted([(len(i[1]), i) for i in options])
            target_path = op_len[0][1] # let's use the smallest one
            cur_source = target_path[1][0][0]
            cur_method = target_path[1][0][1]
            for i in range(0, len(target_path[1]) - 1):
                cur_source = target_path[1][i+1][0]
                cur_method = target_path[1][i+1][1]
                cur_target = target_path[1][i][0]
                target_summarized[len(target_summarized)] = {'source_variables': [cur_source], 'target_variables': [cur_target], 'method': cur_method}
                cur_method = target_path[1][i][1]
                cur_source = target_path[1][i][0]
            cur_target = target
            target_summarized[len(target_summarized)] = {'source_variables': [cur_source], 'target_variables': [cur_target], 'method': cur_method}
        # the values are compiled to reduce the number of queries
        index = list()
        compiled_targets = dict()
        for i in target_summarized.values():
            query = (i['source_variables'][0], i['method'])
            if query in compiled_targets:
                compiled_targets[query] += i['target_variables']
            else:
                compiled_targets[query] = i['target_variables']
                index.append(query)
        return [(i, compiled_targets[i]) for i in index]

    def reference_check(self, columns):
        """
        Checks if the needed references can be found.

        @param columns: the columns we have currently
        """
        def _flatten(ll):
            ret = list()
            for i in ll:
                if type(i) is list:
                    ret += i
                else:
                    ret.append(i)
            return ret
        missing_references = list(set(_flatten([i.reference for i in self.sources])) - set(columns))
        if len(missing_references) > 0: #if there are some reference columns which we dont have
            compiled_targets = self._minimum_mapping(columns, missing_references)
            index_ref = 0
            for source, target_cols in compiled_targets:
                source_cols, source_method = source
                if self.verbose:
                    print("|- Dependency: {} -> '{}' ({})".format(source_cols, "', '".join(target_cols), source_method))
                self.sources.insert(index_ref, source_method(source_cols, target_cols, self._reference_engines[source_method]))
                index_ref += 1

    def collect(self):
        """
        Collects the data each by chunk.

        For each chunk:
        1. add new columns from the dependency checks
        2. add new columns requested
        """
        start = time.time()
        chunk_id = 0
        for i in self._collect():
            yield i
            chunk_id += 1
            if self.stop_after_chunk is not None and chunk_id >= self.stop_after_chunk:
                return
        if self.verbose:
            print('- Extraction took {:.2f}s'.format(time.time() - start))

    def collect_all(self, filtering_function=None):
        """
        Collect all the data, does the filtering function and returns the complete data frame.

        @param filtering_function (default None): function that will be called with the dataframe (it must return the dataframe)
        """
        all_df = list()
        for i in self.collect():
            if filtering_function:
                i = filtering_function(i)
            all_df.append(i)
        return pd.concat(all_df)

    def collect_to_file(self, output_file, filtering_function=None, ignore_file_exists=False, sep=',', index=False, return_dataset=True):
        """
        Collects all the data, does the filtering function on the data and saves it to file. After saving the dataframe may be returned with parameter 'return_dataset'.

        @param output_file: output file
        @param filtering_function (default None): function that will be called with the dataframe (it must return the dataframe)
        @param sep: separator for the output file
        @param return_dataset (default True): if the dataset is going to be returned after this function
        """
        if os.path.isfile(output_file) and not ignore_file_exists:
            raise ObtainDataError('Output file already exists: "{}".'.format(output_file))
        start = time.time()
        if return_dataset:
            all_df = self.collect_all(filtering_function)
            if self.verbose:
                print('|- Saving file...', end='')
            all_df.to_csv(output_file, sep=sep, index=index)
        else:
            first_save = True
            for i in self.collect():
                if filtering_function:
                    i = filtering_function(i)
                if first_save:
                    i.to_csv(output_file, sep=sep, index=index)
                    first_save = False
                else:
                    i.to_csv(output_file, sep=sep, index=index, mode='a', header=False)
            print('Dataset', end='')
        if self.verbose:
            print(' saved! Dataframe processing took {:.2f}s'.format(time.time() - start))
        if return_dataset:
            return all_df

    def _collect(self):
        """
        Internal handling of the chunk collection.
        """
        for chunk in self.database_file_handler():
            start_chunk = time.time()
            # if it is a first run we need a column dependency check
            if not self.checked:
                dependency_check = time.time()
                self.reference_check(chunk.columns.values)
                self.checked = True
                if self.verbose:
                    print('|- Dependency resolution in {:.2f}s'.format(time.time() - dependency_check))
            # for each source of data (this will include dependencies)
            for d in self.sources:
                start_time = time.time()
                if self.verbose:
                    print("|- Collecting '{}'".format(d), end='\r')
                ndf = d.obtain_data(chunk[d.reference]) #obtain the data
                internal_time = time.time()
                # print(chunk.columns.values)
                # print(chunk.head())
                # print(ndf.columns.values)
                # print(ndf.head())
                try:
                    chunk = chunk.merge(ndf, on=d.reference, how='left', copy=False, validate='many_to_one') #merge the data using the reference variables
                except pd.errors.MergeError as e:
                    raise ObtainDataError("Data extractor '{}' failed. There are duplicate elements. Please disable it. Duplicated elements are '{}'.".format(d, "', '".join([str(i) for i in ndf[d.reference][ndf[d.reference].duplicated(keep=False)].unique()]))) from pd.errors.MergeError()
                if self.verbose:
                    print("|- Source '{}' took {:.2f}s (internal processing {:.2f}s)".format(d, time.time() - start_time, time.time() - internal_time))
            if self.verbose:
                print("- Chunk took {:.2f}s".format(time.time() - start_chunk))
            yield chunk

