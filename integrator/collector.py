
import time
from integrator.util import ObtainDataError


class DataCollector:
    """
    Main class for data collection, this class will handle all the others.

    @param database_file_handler: this is a function that yields data blocks
    @param sources: the difference data sources used
    @param reference_sources (list of DBMapping classes): these are the classes that map different reference variables
    @param reference_engines (list of sqlalchemy engines): the engines to be used by the respective list of reference sources
    @param verbose: output some (minimal) verbose information
    """
    def __init__(self, database_file_handler, sources=None, reference_sources=None, reference_engines=None, verbose=False):
        self.database_file_handler = database_file_handler
        if type(sources) is not list: #we are avoiding issues here
            sources = [sources]
        self.sources = sources
        self.checked = False
        self.verbose = verbose
        if reference_sources and reference_engines: #if we possibly doing mapping we need the same number of reference mappers and engine to use with them
            if len(reference_sources) != len(reference_engines):
                raise ObtainDataError('The references sources and engines have different length.')
            self._reference_engines = {i: j for i, j in zip(reference_sources, reference_engines)}
        else:
            self._reference_engines = None
        self._build_reference_graph(reference_sources)

    def _build_reference_graph(self, reference_sources):
        """
        This method builds a graph for future matching of columns.

        @param reference_sources: the references used for dependency resolution
        """
        graph = dict()
        if not reference_sources:
            self.reference_graph = None
            return
        for i in reference_sources:
            for a in i.AVAILABLE:
                if a not in graph:
                    graph[a] = {'neighbours': dict()}
                for b in i.AVAILABLE:
                    if a == b:
                        continue
                    if b not in graph[a]['neighbours']:
                        graph[a]['neighbours'][b] = list()
                    graph[a]['neighbours'][b].append(i)
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
        compiled_targets = dict()
        for i in target_summarized.values():
            query = (i['source_variables'][0], i['method'])
            if query in compiled_targets:
                compiled_targets[query] += i['target_variables']
            else:
                compiled_targets[query] = i['target_variables']
        return compiled_targets

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
            for source, target_cols in compiled_targets.items():
                source_cols, source_method = source
                self.sources.insert(0, source_method(source_cols, target_cols, self._reference_engines[source_method]))

    def collect(self):
        """
        Collects the data each by chunk.

        For each chunk:
        1. add new columns from the dependency checks
        2. add new columns requested
        """
        for chunk in self.database_file_handler():
            start_chunk = time.time()
            # if it is a first run we need a column dependency check
            if not self.checked:
                dependency_check = time.time()
                self.reference_check(chunk.columns.values)
                self.checked = True
                if self.verbose:
                    print('|- Dependency resolution in {}s'.format(time.time() - dependency_check))
            # for each source of data (this will include dependencies)
            for d in self.sources:
                start_time = time.time()
                if self.verbose:
                    print("|- Collecting '{}'".format(d))
                ndf = d.obtain_data(chunk[d.reference]) #obtain the data
                internal_time = time.time()
                # print(chunk.columns.values)
                # print(chunk.head())
                # print(ndf.columns.values)
                # print(ndf.head())
                chunk = chunk.merge(ndf, on=d.reference, how='left', copy=False) #merge the data using the reference variables
                if self.verbose:
                    print("|- Internal processing took {}s".format(time.time() - internal_time))
                    print("|- Source '{}' took {}s".format(d, time.time() - start_time))
            if self.verbose:
                print("- Chunk took {}s".format(time.time() - start_chunk))
            yield chunk

