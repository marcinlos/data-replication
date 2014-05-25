
from pyevolve import Selectors
from pyevolve.Migration import MPIMigration
from random import choice as rand_choice
from mpi4py import MPI


comm = MPI.COMM_WORLD
rank = comm.rank
is_root = comm.rank == 0


class CustomMPIMigrator(MPIMigration):
    ''' Kind of a necessary evil workaround. The problem is Pyevolve's support
    for migration seems not really well-thought out - pickle does not allow
    instancemethod/lambda/staticmethod serialization (well... all right, I can
    understand why is that a reasonable behaviour), so sending genome through
    network is a bit cumbersome, as it has some function slots with a need of
    "personalized" data (the problem). The workaround is to clear these slots
    before serialization, and restoring (using local data) at the other side.

    Unfortunetely, MPIMigration does not really provide convenient way to add
    such hook - hence the need for this class. It duplicates some code of
    MPIMigration, but I don't really see a way to do it cleaner.
    '''

    def __init__(self, gra):
        super(CustomMPIMigrator, self).__init__()
        self.gra = gra
        self.selector.set(Selectors.GRouletteWheel)

    def beforeSerialization(self, genome):
        ''' Invoked before serialization, clears function slots of genome
        '''
        genome.clearRules()

    def afterSerialization(self, genome):
        ''' Invoked after deserialization, restores function slots '''
        genome.setRules(self.gra)

    def gather_bests(self):
        # a bit weird construct, but that's how applyFunctions work,
        # Pyevolve's code is full of usages of this "idiom"
        for it in self.selector.applyFunctions(self.GAEngine.internalPop,
                                 popID=self.GAEngine.currentGeneration):
            best_guy = it

        self.beforeSerialization(best_guy)
        self.all_stars = self.comm.gather(sendobj=best_guy, root=0)
        self.afterSerialization(best_guy)

        if is_root:
            for genome in self.all_stars:
                self.afterSerialization(genome)

    def exchange(self):
        if not self.isReady():
            return

        pool_to_send = self.selectPool(self.getNumIndividuals())

        for genome in pool_to_send:
            self.beforeSerialization(genome)

        pool_received = self.comm.sendrecv(sendobj=pool_to_send,
                                            dest=self.dest,
                                            sendtag=0,
                                            recvobj=None,
                                            source=self.source,
                                            recvtag=0)
        for genome in pool_to_send:
            self.afterSerialization(genome)

        for genome in pool_received:
            self.afterSerialization(genome)

        population = self.GAEngine.getPopulation()

        pool = pool_received
        for i in xrange(self.getNumReplacement()):
            if len(pool) <= 0:
                break

            choice = rand_choice(pool)
            pool.remove(choice)

            # replace the worst
            population[len(population) - 1 - i] = choice

        self.gather_bests()
