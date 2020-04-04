use noria::DataType;
use std::collections::HashSet;
use std::future::Future;
use tower_util::ServiceExt;
use trawler::UserId;

pub(crate) async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(crate::Conn, bool), failure::Error>
where
    F: 'static + Future<Output = Result<crate::Conn, failure::Error>> + Send,
{
    let c = c.await?;
    let ids: Vec<_> = c
        .view("comments_1")
        .await?
        .ready_oneshot()
        .await?
        .lookup(&[DataType::from(0i32)], true)
        .await?
        .into_iter()
        .map(|mut row| vec![row.take("id").unwrap()])
        .collect();

    let mut comments = Vec::new();
    let mut users = HashSet::new();
    let mut stories = HashSet::new();
    for comment in c
        .view("comments_2")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(ids, true)
        .await?
    {
        let mut comment = comment.into_iter().last().unwrap();
        comments.push(comment.take("id").unwrap());
        users.insert(comment.take("user_id").unwrap());
        stories.insert(comment.take("story_id").unwrap());
    }

    if let Some(uid) = acting_as {
        let mut cview = c.view("comments_3").await?;
        // TODO: multi-lookup
        for story in &stories {
            cview
                .ready_and()
                .await?
                .lookup(&[uid.into(), story.clone()], true)
                .await?;
        }
    }

    let _ = c
        .view("comments_4")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(users.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    let authors: HashSet<_> = c
        .view("comments_5")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(stories.into_iter().map(|v| vec![v]).collect(), true)
        .await?
        .into_iter()
        .map(|row| row.into_iter().next().unwrap().take("user_id").unwrap())
        .collect();

    if let Some(uid) = acting_as {
        let mut cview = c.view("comments_6").await?;
        // TODO: multi-lookup
        for comment in comments {
            cview
                .ready_and()
                .await?
                .lookup(&[uid.into(), comment], true)
                .await?;
        }
    }

    let _ = c
        .view("comments_7")
        .await?
        .ready_oneshot()
        .await?
        .multi_lookup(authors.into_iter().map(|v| vec![v]).collect(), true)
        .await?;

    Ok((c, true))
}
