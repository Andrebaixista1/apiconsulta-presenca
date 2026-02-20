let tail = Promise.resolve();
let running = false;
let waiting = 0;

async function runInConsultationQueue(task) {
  if (typeof task !== "function") {
    throw new Error("task precisa ser uma funcao");
  }

  waiting += 1;
  const run = tail.then(async () => {
    waiting = Math.max(0, waiting - 1);
    running = true;
    try {
      return await task();
    } finally {
      running = false;
    }
  });

  tail = run.catch(() => {});
  return run;
}

function getConsultationQueueStats() {
  return {
    running,
    waiting,
  };
}

module.exports = {
  runInConsultationQueue,
  getConsultationQueueStats,
};
