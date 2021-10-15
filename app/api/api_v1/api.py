from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from .endpoints import (module, attachment, user, company, group, role,
                        default_permission, sharing_rule, territory,
                        profile, data, auth)

app = FastAPI()

app.add_middleware(CORSMiddleware,
                   allow_origins=['*'],
                   #    allow_origin_regex='\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}',
                   allow_credentials=True,
                   allow_methods=["*"],
                   allow_headers=["*"])

app.include_router(module.router, prefix='/Module', tags=['Modules'])
app.include_router(attachment.router, prefix='/attachment',
                   tags=['attachments'])
app.include_router(user.router, prefix='/user', tags=['users'])
app.include_router(territory.router, prefix='/territory', tags=['territories'])
app.include_router(company.router, prefix='/company', tags=['company'])
app.include_router(group.router, prefix='/group', tags=['groups'])
app.include_router(profile.router, prefix='/profile', tags=['profiles'])
app.include_router(role.router, prefix='/role', tags=['roles'])
app.include_router(default_permission.router,
                   prefix='/default_permission', tags=['default permissions'])
app.include_router(sharing_rule.router,
                   prefix='/sharing_rule', tags=['sharing rules'])
app.include_router(data.router, prefix='/data', tags=['data'])
app.include_router(auth.router, prefix='/auth', tags=['auth'])


if __name__ == '__main__':
    # create_module_collections()
    uvicorn.run(app, debug=True, port=8080, host='0.0.0.0', reload=True)
