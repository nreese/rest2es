# REST2ES
Application for migrating REST endpoints into ElasticSearch.

### Why not use logstash?
Logstash HTTP input plugin can only be used to fetch a static URL. What about cases where the URL needs to be updated between requests?

This application provides the mechanisms necessary to crawl a web service. The context from the previous request is passed to the logic for creating the next URL.

# Integrating REST endpoint
Add new configuration object to configs array

```
{
  rest: {
    /**
     * String identify file for storing REST context. 
     * File is used to provide initial context upon process start.
     * File is updated after each successful http-request/es-insert cycle.
     */
    contextFilename: '',
    /**
     * Sleep period when REST call returns no data or an error occurs
     */
    sleep: 300000,
    /**
     * Create the URL for next GET request.
     * Use the context from the previous response to craft the URL address
     *
     * @method next
     * @param prevContext {Object} Previous response context containing the min and max values for each property defined in watchlist.
     * @return {string} next URL to fetch
    next: function(prevContext) {},
    /**
     * Array of strings containing property names that will be placed in the prevContext object utilized by the next function.
     */
    watchlist: []
  },
  es: {
    host: 'ES ip address',
    index: 'ES index',
    type: 'ES index type'
  },
  /**
   * Some REST endpoints do not return an Array of rows.
   * This function provides the ability to traverse the response and return an Array of rows.
   *
   * @method extractRows
   * @param respBody {Object} Response body
   * @return {Array}
   */
  extractRows: function(respBody) {},
  /**
   * Transform REST document into ElasticSearch document
   *
   * @method transfrom
   * @param doc {Object} row returned from REST web service array.
   * @param callback {function} Function to call when transformation is complete.
   */
  transform: function(doc, callback) {}
}
```