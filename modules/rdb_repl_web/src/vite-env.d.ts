declare module "*.css";
declare module "*.sql?raw" {
    const source: string;
    export default source;
}
