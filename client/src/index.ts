import { Data, tableFromIPC } from "apache-arrow"
import { assert } from "console"
import danfo from "danfojs"
import { DataFrame } from "danfojs"
import { Some, None, Option, some, none, isNone, isSome } from "fp-ts/Option"
import { cons } from "fp-ts/lib/ReadonlyNonEmptyArray"

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
        return [mat.length].concat(dim(mat[0]));
    } else {
        return [];
    }
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

  const query_body = { sql: query }

  const table_response = await fetch(API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query_body })
  })

  const response_context_type = table_response.headers.get('Content-Type')
  if (response_context_type !== ARROW_CONTENT_TYPE) {
    const body = await table_response.text()
    console.error(body)
  } else {
    const table = tableFromIPC(await table_response.arrayBuffer())
    // console.log(table)
    // console.log(table.schema.fields)
    let df: Option<DataFrame> = none
    table.schema.fields.forEach((field) => {
      const column = table.getChild(field.name)
      assert(column != null)
      // console.log(`Adding column ${field.name} with length ${column.length}`)
      let val = Array.from(column.toArray())
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

// https://stackoverflow.com/questions/4981891/node-js-equivalent-of-pythons-if-name-main
if (typeof require !== 'undefined' && require.main === module) {
  main()
}

