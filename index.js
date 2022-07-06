const PgBoss = require('pg-boss');
const dotenv = require('dotenv');
dotenv.config();

const dbURl = `${process.env.DATABASE}://${process.env.DB_USERNAME}:${process.env.DB_PASSWORD}@${process.env.HOST}:${process.env.PORT}/${process.env.DB_NAME}`;
console.log({dbURl});

/*      Connecting PgBoss to postres        */ 
const boss = new PgBoss(dbURl);
// const boss = new PgBoss('postgres://firstuser:mypass@localhost:5433/mynewdb');

const ordersPayloadData = [
    {
        email: "billybob1@veganplumbing.com",
        name: "Billy Bob1"
    },
    {
        email: "billybob2@veganplumbing.com",
        name: "Billy Bob2"
    },
    {
        email: "billybob3@veganplumbing.com",
        name: "Billy Bob3"
    },
    {
        email: "billybob4@veganplumbing.com",
        name: "Billy Bob4"
    },
    {
        email: "billybob5@veganplumbing.com",
        name: "Billy Bob5"
    },
]
const queue = 'Pizza order queue';

boss.on('monitor-states',(success, error) => console.log('monitor-states',success,error))
// boss.on('wip',(success, error) => console.log('wip',success,error))
boss.on('stopped',(success, error) => console.log('stopped',success,error))

boss.on('error', error => console.error('boss raised error',{error}));
boss.on('connected', (success, error) => console.log('connected',success,error));
boss.start('monitor-states', (success, error) => console.log('started',success,error));

let workid = ''


const pizzaOrderQueue = async() => {
    try {
    await boss.start();
        console.log({ queue, ordersPayloadData })
        for(let order of ordersPayloadData){
            const jobId = await boss.send(queue, order)
            console.log('54', { jobId })
        }
        workid = await boss.work(queue, { 
            teamSize: 15,
            // batchSize: 5 ,
        },someAsyncJobHandler)
            
    } catch (error) {
        console.log('61->',{error})
    }
}
const someAsyncJobHandler = async(job) => {

    console.log(`job ${job.id} received with data:`);
    console.log('Feeling sleepy');
    await sleep(5000);
    console.log('Awake now');
    return {id: job.id, data:job.data, response : 'success'}
  }
const sleep =(ms) =>{
    return new Promise(resolve => setTimeout(resolve, ms));
}
pizzaOrderQueue();

process.on('SIGTERM', () => {
    console.info('SIGTERM signal received. Request found to kill current server --');
    boss.offWork({id: workid})
    // boss.stop({graceful: false})
});
process.on('SIGINT', () => {
    console.info('SIGKILL signal received. Request found to kill current server --');
    boss.offWork({id: workid})
    boss.stop({graceful: false})
});