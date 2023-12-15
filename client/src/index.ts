import { Data, tableFromIPC } from "apache-arrow"
import { assert } from "console"
import danfo from "danfojs"
import { DataFrame } from "danfojs"
import { ArrayType1D } from "danfojs/dist/danfojs-base/shared/types"
import { Some, None, Option, some, none, isNone, isSome } from "fp-ts/Option"
import CborRpcCaller from "./cborpc"

const API_URL = 'http://127.0.0.1:8000/query'
const ARROW_CONTENT_TYPE = 'application/vnd.apache.arrow.file'

interface QueryBody {
  sql: string
}


// From https://gist.github.com/srikumarks/4303229
// Assumes a valid matrix and returns its dimension array.
// Won't work for irregular matrices, but is cheap.
function dim(mat) {
  if (mat instanceof Array) {
    return [mat.length].concat(dim(mat[0]))
  } else {
    return []
  }
}

const doQuery = async () => {
  // const query = `--sql
  // SELECT t.name    as tag_name,
  //      post_count
  // FROM booru.artists
  //         INNER JOIN booru.artist_tags_assoc ata on artists.id = ata.artist_id
  //         INNER JOIN booru.tags t on ata.tag_id = t.id
  //         -- INNER JOIN booru.tag_post_counts tpc on t.id = tpc.tag_id
  //         INNER JOIN booru.view_posts_count_illustration_only tpc on t.id = tpc.tag_id
  // GROUP BY t.id, artist_id, t.name, post_count;
  // `
  const query = `--sql
WITH artist AS (SELECT *
                FROM booru.artists_with_n_posts
                ORDER BY random()
                LIMIT 1),
     artist_posts_with_tags_id AS (SELECT ap.post_id,
                                          ap.score,
                                          ap.fav_count,
                                          ap.tag_ids,
                                          ap.created_at,
                                          ap.file_url,
                                          ap.preview_file_url
                                   FROM booru.view_modern_posts_illustration_only_extra ap
                                   WHERE ap.tag_ids && ARRAY(SELECT tag_id FROM artist)),

     -- translate tag id to tag name
     artist_posts_with_tags AS (SELECT ap.post_id,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 1) as artist_tags,
                                       ap.score,
                                       ap.fav_count,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 0) as general_tags,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 3) as copyright_tags,
                                       (SELECT array_agg(name)
                                        FROM booru.tags
                                        WHERE id = ANY (tag_ids)
                                          AND category = 4) as characters_tags,
                                       ap.created_at,
                                       ap.file_url,
                                       ap.preview_file_url
                                FROM artist_posts_with_tags_id as ap)
SELECT *
FROM artist_posts_with_tags
LIMIT 50;

  `

  const query_body = { sql: query }

  const table_response = await fetch(API_URL, {
    method: 'POST',
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/vnd.apache.arrow.file",
    },
    body: JSON.stringify({ query_body })
  })

  const response_context_type = table_response.headers.get('Content-Type')
  if (response_context_type !== ARROW_CONTENT_TYPE) {
    const body = await table_response.text()
    console.error(body)
  } else {
    const table = tableFromIPC(await table_response.arrayBuffer())
    console.log(table)
    console.log(table.schema.fields)
    let df: Option<DataFrame> = none
    table.schema.fields.forEach((field) => {
      const column = table.getChild(field.name)
      assert(column != null)
      // console.log(`Adding column ${field.name} with length ${column.length}`)
      let val = Array.from(column.toArray()) as ArrayType1D
      if (typeof val[0] === 'bigint') {
        val = val.map((x) => Number(x))
      }
      if (isNone(df)) {
        df = some(new DataFrame({ [field.name]: val }))
      } else {
        df.value.addColumn(field.name, val, { inplace: true })
      }
    })
    if (isSome(df)) {
      df.value.describe().print()
    }
  }
}

const doRpc = async () => {
  const WS_URL = "ws://127.0.0.1:8000/ws"
  const caller = new CborRpcCaller(WS_URL)
  while (!caller.wsOpend()) {
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  // const result = await caller.call("log", "fuck", "me")
  const code = `
  function fn(){
    const node = LiteGraph.createNode("basic/Example")
    graph.add(node)
    return node.id
  }
  fn()
  `
  const result = await caller.call("eval", code)
  console.log(result)
  caller.close()
}

const main = async () => {
  await doQuery()
  // await doRpc()
}

// https://stackoverflow.com/questions/4981891/node-js-equivalent-of-pythons-if-name-main
if (typeof require !== 'undefined' && require.main === module) {
  main()
}

