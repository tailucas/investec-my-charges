const invokeWebhook = async (transaction) => {
    const updateResponse = await fetch(
      process.env.webhook_endpoint,
      {
        method: "POST",
        body: JSON.stringify(transaction),
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": process.env.webhook_api_key,
        },
      }
    );
    return updateResponse.json();
};

// It has a limited execution time, so keep any code short-running.
const beforeTransaction = async (authorization) => {
    console.log(authorization);
    return true;
};

// This function runs after an approved transaction.
const afterTransaction = async (transaction) => {
    console.log(transaction);
    const response = await invokeWebhook(transaction);
    console.log(response);
};

// This function runs after a declined transaction
const afterDecline  = async (transaction) => {
    console.log(transaction);
};