---
title: 'Import Airtable to NocoDB'
description: 'Import Airtable to NocoDB'
position: 1150
category: 'Product'
menuTitle: 'Import Airtable to NocoDB'
---

NocoDB allows a complete import of your Airtable to any MySQL, Postgres, SQLite and SQL server databases within minutes.

<alert>
Import from Airtable is in beta version. See <a href="https://github.com/nocodb/nocodb/discussions/2122" target="_blank">Importing Airtable To NocoDB</a> for the migration notes.
</alert>

## Get Airtable Credentials

<alert>
You need to retrieve API key and Shared Base ID / URL from Airtable.
</alert>

### Retrieve API Key

- Copy your Airtable API Key from [Airtable Accounts](https://airtable.com/account) page
  
  ![Screenshot 2022-05-16 at 1 50 07 PM](https://user-images.githubusercontent.com/86527202/168569905-48c16d6d-c44a-4337-be49-0ac3dc1f7b75.png)

### Retrieve Share Base ID / URL

See [here](https://support.airtable.com/hc/en-us/articles/205752117-Creating-a-base-share-link-or-a-view-share-link#basesharelink) for detailed procedures.

1. Open `Share` menu in your Project / Base
    ![Screenshot 2022-05-16 at 3 47 27 PM](https://user-images.githubusercontent.com/86527202/168572054-533b8c19-d76e-4add-b876-f1e0570ac33c.png)

2. Open tab `Share Publicly`

3. Enable `Turn on full base access`

4. Copy generated shared base URL
    ![Screenshot 2022-05-16 at 3 41 54 PM](https://user-images.githubusercontent.com/86527202/168572062-5dee065d-2394-426d-8f43-77ecc0c9b73f.png)



## Import Airtable to NocoDB

<alert>
Prerequisites: <br/> - A NocoDB Project <br/> - Airtable Credentials
</alert>

Below are 3 simple steps
1. Go to your Project and Click `Add / Import` and click `Airtable`.
  ![image](https://user-images.githubusercontent.com/35857179/168772072-937b037b-32b3-4e5b-b982-5ee4b9a4959c.png)
  ![image](https://user-images.githubusercontent.com/35857179/168773192-f3ef9d36-3329-4324-ae25-989b611f66bf.png)
2. Input API key & Shared Base ID / URL (retrieved from `Get Airtable Credentials` above).
  ![image](https://user-images.githubusercontent.com/35857179/168779663-5bb1dac8-01bd-43fb-8638-318a66a0f4bf.png)
3. Click `Import` and you will see the status.
  ![image](https://user-images.githubusercontent.com/35857179/168779906-6163b23e-4bcc-4991-8a77-b2fa94e5dcf3.png)
