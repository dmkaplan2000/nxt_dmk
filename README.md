# nxt_dmk - Fork of NXT by DMK for me to play with

This is a fork of the nxt server maintained by Jean-Luc Picard on [bitbucket](https://bitbucket.org/JeanLucPicard/nxt.git). It is not intended to be used by anyone in production situations, but rather is for me to play with and test new features.  All my new features are found on the branch `feature/dmk`.

# New features

## Attachment schema

For the time being, the main new feature is some code to place attachment info into special tables to make querying attachments outside of java/NXT possible. The code for this is in [AttachmentSchema.java](https://github.com/dmkaplan2000/nxt_dmk/blob/feature/dmk/src/java/nxt/AttachmentSchema.java). To use this, run `compile.sh` and then run (while the nxt server is not running) `init_attachment_schema.sh` . This will initialize tables in a schema "attachment" with data from attachments currently in the blockchain.  This data will not be updated by the server and therefore, `init_attachment_schema.sh` must be rerun every time one wants access to up to date data.


