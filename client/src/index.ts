import { tableFromIPC } from "apache-arrow";
import { assert } from "console"

const API_URL = 'http://127.0.0.1:8000/query'
const ARROW_CONTENT_TYPE = 'application/vnd.apache.arrow.file'

interface QueryBody {
  sql: string
}

const main = async () => {
  const query = `--sql
  SELECT t.name    as tag_name,
       post_count
  FROM booru.artists
          INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
          INNER JOIN booru.tags t on ata.tag_id = t.id
          -- INNER JOIN booru.tag_post_counts tpc on t.id = tpc.tag_id
          INNER JOIN booru.view_posts_count_illustration_only tpc on t.id = tpc.tag_id
  GROUP BY t.id, artist_id, t.name, post_count;
  `

  const query_body = {sql: query}

  const table = await fetch(API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({query_body})
  })

  const response_context_type = table.headers.get('Content-Type')
  if (response_context_type !== ARROW_CONTENT_TYPE) {
    const body = await table.text()
    console.error(body)
  } else {
    const table_json = tableFromIPC(await table.arrayBuffer())
    console.log(table_json)
  }
}

// https://stackoverflow.com/questions/4981891/node-js-equivalent-of-pythons-if-name-main
if (typeof require !== 'undefined' && require.main === module) {
  main()
}

