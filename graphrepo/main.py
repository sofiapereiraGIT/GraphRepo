from py2neo import Graph
from constants import Constants
from drill import Drill

CT = Constants()

def init_graph():
  """Configures the graph + connection
  :returns: graph
  """
  repo_graph = Graph(host=CT.DB_URL, user=CT.DB_USER, password=CT.DB_PWD)
  return repo_graph

def main():
  repo_graph = init_graph()
  driller = Drill()
  commits, repo = driller.drill()

  # index in neo4j
  for com in commits:
    com.index_all(repo_graph, repo)

if __name__ == '__main__':
  main()
