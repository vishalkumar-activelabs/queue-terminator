const PgBoss = require("pg-boss");
const dotenv = require("dotenv");
dotenv.config();

const dbURl = `${process.env.DATABASE}://${process.env.DB_USERNAME}:${process.env.DB_PASSWORD}@${process.env.HOST}:${process.env.PORT}/${process.env.DB_NAME}`;
console.log({ dbURl });

let breakOnGoingQueue = false;
/*      Connecting PgBoss to postres        */
const boss = new PgBoss(dbURl);
// const boss = new PgBoss('postgres://firstuser:mypass@localhost:5433/mynewdb');

const queue = "email-daily-digest";
const batchSize = 20;
const ordersPayloadData = [
  {
    email: "billybob1@veganplumbing.com",
    name: "Billy Bob1",
  },
  {
    email: "billybob2@veganplumbing.com",
    name: "Billy Bob2",
  },
  {
    email: "billybob3@veganplumbing.com",
    name: "Billy Bob3",
  },
  {
    email: "billybob4@veganplumbing.com",
    name: "Billy Bob4",
  },
  {
    email: "billybob5@veganplumbing.com",
    name: "Billy Bob5",
  },
];

const pizzaOrderQueue = async () => {
  try {
    await boss.start();
    for (let order of ordersPayloadData) {
      const jobId = await boss.send(queue, order);
      console.log("54", { jobId });
    }
    const jobs = await boss.fetch(queue, batchSize);
    console.log({ jobs });
    if (!jobs) {
      console.log("No jobs found");
      return;
    }
    for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i]
    if(breakOnGoingQueue){
        console.log("Queue is breaked now.")
        break;
    }
        try {
            await someAsyncJobHandler(job)
            await boss.complete(job.id)
        } catch(err) {
            await boss.fail(job.id, err)
        }
    }
  } catch (error) {
    console.log("61->", { error });
  }
};
pizzaOrderQueue();

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

// process.on('SIGTERM', () => {
//     console.info('SIGTERM signal received. Request found to kill current server --');
//     // boss.offWork({id: workid})
//     // boss.stop({graceful: false})
// });
process.on('SIGINT', async() => {
    console.info('SIGKILL signal received. Request found to kill current server --');
    // await boss.offWork(queue)
    // const qsiez = await boss.getQueueSize(queue)

    // console.log({qsiez})
    breakOnGoingQueue = true;
    // await boss.stop()
    // await boss.clearStorage()
});