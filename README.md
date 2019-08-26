# smtx_spark
 Programming spark and concepts
 
 1. Qual o objetivo do comando cache() em Spark?

É um mecanismo para acelerar aplicações que acessam o mesmo RDD várias vezes. Um RDD que não é armazenado em cache ou com ponto é reavaliado cada vez que uma ação é invocada nesse RDD. Existem duas chamadas de função para armazenar em cache um RDD: cache() e persist (StorageLevel). A diferença entre eles é que cache() armazenará em cache o RDD na memória, enquanto persist(StorageLevel) pode armazenar em cache na memória ou no disco. Note que persist() sem argumento é equivalente a cache().


2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Uma das principais limitações do MapReduce é que ele persiste o conjunto de dados completo após a execução de cada job. Isso incorre em um grande fluxo de entrada e saída de dados em disco e em rede. Já no Spark, quando a saída de uma operação precisa ser alimentada em outra operação, o Spark passa os dados diretamente sem persistir.

A principal inovação do Spark foi a introdução de uma abstração de cache na memória. Isso torna o Spark ideal para cargas de trabalho em que várias operações acessam os mesmos dados de entrada. Os usuários podem instruir o Spark a armazenar em cache os conjuntos de dados de entrada na memória, para que não precisem ser lidos do disco para cada operação.

Outro fato é que o Spark consegue iniciar tarefas muito mais rapidamente. O MapReduce inicia uma nova JVM para cada tarefa, o que pode levar segundos para carregar JARs, JIT, analisar a configuração XML etc. O Spark mantém uma JVM executora em execução em cada nó.

