export const debug = (...args: any) => {
  if (process.env.DEBUG) {
    console.log(...args);
  }
};
