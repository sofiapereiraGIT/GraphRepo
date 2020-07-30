"""This module is the wild wild west of batch indexing :-)
In contains all Neo4j queries for indexing the data in batches.
More documentation will follow soon.
"""
from datetime import datetime
import hashlib
import subprocess
import graphrepo.utils as utl


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def index_commits(graph, commits, batch_size=100):
    query = """
    UNWIND {commits} AS c
    MERGE (:Commit { hash: c.hash,
                     commit_hash: c.commit_hash,
                     timestamp: c.timestamp,
                     is_merge: c.is_merge,
                     project_id: c.project_id,
                     message: c.message,
                     dmm_unit_complexity: c.dmm_unit_complexity,
                     dmm_unit_interfacing: c.dmm_unit_interfacing,
                     dmm_unit_size: c.dmm_unit_size
                     })
    """
    for b in batch(commits, batch_size):
        graph.run(query, commits=b)
    create_index_commits(graph)


def index_parent_commits(graph, parents, batch_size=100):
    query = """
    UNWIND {ac} AS a
    MATCH (x:Commit),(y:Commit)
    WHERE x.hash = a.parent_hash AND y.hash = a.child_hash
    MERGE (x)-[r:Parent{}]->(y)
    """
    for b in batch(parents, batch_size):
        graph.run(query, ac=b)


def index_authors(graph, authors, batch_size=100):
    query = """
    UNWIND {authors} AS a
    MERGE (:Developer { hash: a.hash,
                        email: a.email,
                        name: a.name
                      })
    """
    for b in batch(authors, batch_size):
        graph.run(query, authors=b)
    create_index_authors(graph)


def index_branches(graph, branches, batch_size=100):
    query = """
    UNWIND {branches} AS a
    MERGE (:Branch { hash: a.hash,
                     name:a.name,
                     project_id: a.project_id})
    """
    for b in batch(branches, batch_size):
        graph.run(query, branches=b)
    create_index_branches(graph)


def index_branch_commits(graph, bc, batch_size=100):
    query = """
    UNWIND {ac} AS a
    MATCH (x:Branch),(y:Commit)
    WHERE x.hash = a.branch_hash AND y.hash = a.commit_hash
    MERGE (x)-[r:BranchCommit{}]->(y)
    """
    for b in batch(bc, batch_size):
        graph.run(query, ac=b)


def index_files(graph, files, batch_size=100):
    query = """
    UNWIND {files} AS f
    MERGE (:File { hash: f.hash,
                   project_id: f.project_id,
                   type:f.type,
                   name: f.name,
                   path: f.path,
                   complexity: f.complexity,
                   current_methods: f.current_methods})
    """
    for b in batch(files, batch_size):
        graph.run(query, files=b)
    create_index_files(graph)


def index_methods(graph, methods, batch_size=100):
    query = """
    UNWIND {methods} AS f
    MERGE (:Method { hash: f.hash,
                     project_id: f.project_id,
                     name: f.name,
                     long_name: f.long_name,
                     file_name: f.file_name,
                     nloc: f.nloc,
                     complexity: f.complexity,
                     parameters: f.parameters
                     })
    """

    for b in batch(methods, batch_size):
        graph.run(query, methods=b)
    create_index_methods(graph)


def index_blame(repo_url, graph, files, batch_size=100):
    blames = []
    files_blames = []

    for file in files:
        cmd = 'cd {repo};' \
              'git blame -w -M -C --line-porcelain "{fpath}"' \
              '| sed -n \'s/^author-mail <//p\' | sed -n \'s/>//p\' | sort | uniq -c | sort -rn' \
            .format(repo=repo_url, fpath=file['path'])

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, stderr=subprocess.PIPE)
        try:
            blame = process.communicate()[0].decode("utf-8").split('\n')
        except:  # ignores errors from files git sees as binary files, for example
            blame = ['']

        blame_dict = {}
        if blame != ['']:
            for line in blame:
                if line != '':
                    lines_developer = line.strip().split()
                    blame_dict[lines_developer[1]] = lines_developer[0]

        b = utl.format_blame(blame_dict, file)
        blames.append(b)
        files_blames.append(utl.format_file_blame(file['hash'], b['hash']))

    query = """
    UNWIND {blames} AS b
    MERGE (:Blame { hash: b.hash,
                   project_id: b.project_id,
                   name: b.name,
                   filename: b.filename,
                   blame: b.blame})
    """
    for b in batch(blames, batch_size):
        graph.run(query, blames=b)
    create_index_blame(graph)

    return files_blames


def index_author_commits(graph, ac, batch_size=100):
    query = """
    UNWIND {ac} AS a
    MATCH (x:Developer),(y:Commit)
    WHERE x.hash = a.author_hash AND y.hash = a.commit_hash
    MERGE (x)-[r:Author{timestamp: a.timestamp}]->(y)
    """
    for b in batch(ac, batch_size):
        graph.run(query, ac=b)


def index_commit_files(graph, cf, batch_size=100):
    query = """
    UNWIND {cf} AS a
    MATCH (x:Commit),(y:File)
    WHERE x.hash = a.commit_hash AND y.hash = a.file_hash
    MERGE (x)-[r:UpdateFile{}]->(y)
    ON CREATE SET r=a['attributes']
    """
    for i, b in enumerate(batch(cf, batch_size)):
        graph.run(query, cf=b)


def index_file_methods(graph, cf, batch_size=100):
    query = """
    UNWIND {cf} AS a
    MATCH (x:File),(y:Method)
    WHERE x.hash = a.file_hash AND y.hash = a.method_hash
    MERGE (x)-[r:Method{}]->(y)
    """
    for b in batch(cf, batch_size):
        graph.run(query, cf=b)


def index_blame_file(graph, files_blames, batch_size=100):
    query = """
    UNWIND {files_blames} AS b
    MATCH (x:Blame),(y:File)
    WHERE x.hash = b.blame_hash AND y.hash = b.file_hash
    MERGE (x)-[r:GitBlame{}]->(y)
    """
    for b in batch(files_blames, batch_size):
        graph.run(query, files_blames=b)


def index_commit_method(graph, cm, batch_size=100):
    query = """
    UNWIND {cf} AS a
    MATCH (x:Commit),(y:Method)
    WHERE x.hash = a.commit_hash AND y.hash = a.method_hash
    MERGE (x)-[r:UpdateMethod]->(y)
    ON CREATE SET r=a['attributes']
    """
    for i, b in enumerate(batch(cm, batch_size)):
        graph.run(query, cf=b)


def create_index_authors(graph):
    query = """
    CREATE INDEX ON :Developer(hash)
    """
    graph.run(query)


def create_index_commits(graph):
    hash_q = """
    CREATE INDEX ON :Commit(hash)
    """
    pid_q = """
    CREATE INDEX ON :Commit(project_id)
    """
    graph.run(hash_q)
    graph.run(pid_q)


def create_index_branches(graph):
    hash_q = """
    CREATE INDEX ON :Branch(hash)
    """
    pid_q = """
    CREATE INDEX ON :Branch(project_id)
    """
    graph.run(hash_q)
    graph.run(pid_q)


def create_index_files(graph):
    hash_q = """
    CREATE INDEX ON :File(hash)
    """
    pid_q = """
    CREATE INDEX ON :File(project_id)
    """
    graph.run(hash_q)
    graph.run(pid_q)


def create_index_methods(graph):
    hash_q = """
    CREATE INDEX ON :Method(hash)
    """
    pid_q = """
    CREATE INDEX ON :Method(project_id)
    """
    graph.run(hash_q)
    graph.run(pid_q)


def create_index_blame(graph):
    hash_q = """
        CREATE INDEX ON :Blame(hash)
        """
    pid_q = """
        CREATE INDEX ON :Blame(project_id)
        """
    graph.run(hash_q)
    graph.run(pid_q)


def index_all(repo_url, graph, developers, commits, parents, dev_commits, branches,
              branches_commits, files, commit_files, methods, file_methods,
              commit_methods, batch_size=100):

    total = datetime.now()

    developers = list({v['hash']: v for v in developers}.values())
    print('Indexing ', len(developers), ' authors')
    start = datetime.now()
    index_authors(graph, developers, batch_size)
    print('Indexed authors in: \t', datetime.now()-start)

    print('Indexing ', len(commits), ' commits')
    start = datetime.now()
    index_commits(graph, commits, batch_size)
    print('Indexed commits in: \t', datetime.now()-start)

    branches = list({v['hash']: v for v in branches}.values())
    branches_commits = list({str(i): i for i in branches_commits}.values())
    print('Indexing ', len(branches), ' branches')
    start = datetime.now()
    index_branches(graph, branches, batch_size)
    index_branch_commits(graph, branches_commits, batch_size)
    print('Indexed branches in: \t', datetime.now()-start)

    files = list({v['hash']: v for v in files}.values())
    print('Indexing ', len(files), ' files')
    start = datetime.now()
    index_files(graph, files, batch_size)
    print('Indexed files in: \t', datetime.now()-start)

    methods = list({v['hash']: v for v in methods}.values())
    print('Indexing ', len(methods), ' methods')
    start = datetime.now()
    index_methods(graph, methods, batch_size)
    print('Indexed methods in: \t', datetime.now()-start)

    filesToBlame = list({v['hash']: v for v in files}.values())
    print('Indexing ', len(filesToBlame), ' blames')
    start = datetime.now()
    files_blames = index_blame(repo_url, graph, filesToBlame, batch_size)
    index_blame_file(graph, files_blames, batch_size)
    print('Indexed blames in: \t', datetime.now() - start)

    parents = list({str(i): i for i in parents}.values())
    print('Indexing ', len(parents), ' parent commits')
    start = datetime.now()
    index_parent_commits(graph, parents, batch_size)
    print('Indexed commits in: \t', datetime.now()-start)

    print('Indexing ', len(dev_commits), ' author_commits')
    start = datetime.now()
    index_author_commits(graph, dev_commits, batch_size)
    print('Indexed author_commits in: \t', datetime.now()-start)

    file_methods = list({str(i): i for i in file_methods}.values())
    print('Indexings ', len(file_methods), ' file_methods')
    start = datetime.now()
    index_file_methods(graph, file_methods, batch_size)
    print('Indexed file_methods in: \t', datetime.now()-start)

    print('Indexing ', len(commit_methods), ' commit_methods')
    start = datetime.now()
    index_commit_method(graph, commit_methods, batch_size)
    print('Indexed commit_methods in: \t', datetime.now()-start)

    print('Indexing ', len(commit_files), ' commit_files')
    start = datetime.now()
    index_commit_files(graph, commit_files, batch_size)
    print('Indexed commit_files in: \t', datetime.now()-start)

    print('Indexing took: \t', datetime.now()-total)


def index_cache(repo_url, graph, cache, batch_size=100):
    total = datetime.now()
    index_authors(graph, list(
        {v['hash']: v for v in cache.data['developers']}.values()), batch_size)
    index_commits(graph, cache.data['commits'], batch_size)
    index_branches(graph, list(
        {v['hash']: v for v in cache.data['branches']}.values()), batch_size)
    index_branch_commits(graph,  list(
        {str(i): i for i in cache.data['branches_commits']}.values()), batch_size)
    index_files(graph, list(
        {v['hash']: v for v in cache.data['files']}.values()), batch_size)
    index_methods(graph, list(
        {v['hash']: v for v in cache.data['methods']}.values()), batch_size)

    filesToBlame = list({v['hash']: v for v in cache.data['files']}.values())
    files_blames = index_blame(repo_url, graph, filesToBlame, batch_size)
    index_blame_file(graph, files_blames, batch_size)

    index_parent_commits(graph, list(
        {str(i): i for i in cache.data['parents']}.values()), batch_size)
    index_author_commits(graph, cache.data['dev_commits'], batch_size)
    index_file_methods(graph, list(
        {str(i): i for i in cache.data['file_methods']}.values()), batch_size)
    index_commit_method(graph, cache.data['commit_methods'], batch_size)
    index_commit_files(graph, cache.data['commit_files'], batch_size)
    print('Indexing took: \t', datetime.now()-total)
